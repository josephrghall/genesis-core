use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, params_from_iter, types::Value};

use crate::{
    api::{bounded_limit, decode_cursor, encode_cursor},
    CoreError, CursorPage, GraphEdge, GraphNode, GraphProjection, IndexedPropertyMatch,
    IndexedRelationship, LifecycleState, Node, NodeId, PropertyFilter, PropertyQuery, PropertySort,
    PropertyValue, ReadCursor, ReadPage, RelationshipProvenance, SearchHit,
};

use super::IndexStore;

impl IndexStore {
    pub(crate) fn query_properties(
        &self,
        query: &PropertyQuery,
    ) -> Result<Vec<IndexedPropertyMatch>, CoreError> {
        self.query_properties_window(query, query.limit.clamp(1, 100_000) as i64, 0)
    }

    pub(crate) fn query_properties_page(
        &self,
        query: &PropertyQuery,
        page: ReadPage,
    ) -> Result<Vec<IndexedPropertyMatch>, CoreError> {
        self.query_properties_window(query, page.sql_limit(), page.sql_offset()?)
    }

    fn query_properties_window(
        &self,
        query: &PropertyQuery,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<IndexedPropertyMatch>, CoreError> {
        if query.key.trim().is_empty() {
            return Err(CoreError::Conflict(
                "an indexed Property query requires a non-empty key".into(),
            ));
        }

        let mut sql = String::from(
            "SELECT p.node_uuid, p.value_kind, p.text_value, p.integer_value,                     p.real_value, p.json_value
             FROM indexed_properties p
             JOIN indexed_nodes n ON n.node_uuid=p.node_uuid
             WHERE p.property_key=?1 AND n.lifecycle_state='active'",
        );
        let mut bindings = vec![Value::Text(query.key.clone())];
        let mut sort_column = None;

        if let Some(filter) = &query.filter {
            let (value_kind, value_column, operator, value) = property_filter_parts(filter)?;
            let kind_position = bindings.len() + 1;
            bindings.push(Value::Text(value_kind.into()));
            sql.push_str(&format!(" AND p.value_kind=?{kind_position}"));
            sort_column = Some(value_column);

            if let Some(value) = value {
                let value_position = bindings.len() + 1;
                bindings.push(value);
                sql.push_str(&format!(
                    " AND p.{value_column} {operator} ?{value_position}"
                ));
            } else if operator != "=" {
                return Err(CoreError::Conflict(
                    "Property range filters require an orderable value kind".into(),
                ));
            }
        }

        let direction = match query.sort {
            PropertySort::Ascending => "ASC",
            PropertySort::Descending => "DESC",
        };
        match sort_column {
            Some(column) => sql.push_str(&format!(
                " ORDER BY p.{column} {direction}, p.node_uuid ASC"
            )),
            None => sql.push_str(&format!(
                " ORDER BY p.value_kind {direction}, p.text_value {direction},                  p.integer_value {direction}, p.real_value {direction},                  p.json_value {direction}, p.node_uuid ASC"
            )),
        }
        let limit_position = bindings.len() + 1;
        bindings.push(Value::Integer(limit));
        sql.push_str(&format!(" LIMIT ?{limit_position}"));
        let offset_position = bindings.len() + 1;
        bindings.push(Value::Integer(offset));
        sql.push_str(&format!(" OFFSET ?{offset_position}"));

        let mut statement = self.connection()?.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(bindings), |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<f64>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;

        let mut matches = Vec::new();
        for row in rows {
            let (node_id, kind, text, integer, real, json) = row?;
            matches.push(IndexedPropertyMatch {
                node_id: decode_node_id(node_id)?,
                value: decode_indexed_value(&kind, text, integer, real, json)?,
            });
        }
        Ok(matches)
    }

    pub(crate) fn list_nodes(&self) -> Result<Vec<Node>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT node_uuid, lifecycle_state FROM indexed_nodes \
             ORDER BY modified_revision DESC, node_uuid",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut nodes = Vec::new();
        for row in rows {
            let (bytes, lifecycle) = row?;
            let lifecycle_state = match lifecycle.as_str() {
                "active" => LifecycleState::Active,
                "tombstoned" => LifecycleState::Tombstoned,
                _ => return Err(corrupt("indexed Node lifecycle is invalid")),
            };
            nodes.push(Node {
                id: decode_node_id(bytes)?,
                lifecycle_state,
            });
        }
        Ok(nodes)
    }

    pub(crate) fn list_nodes_page(&self, page: ReadPage) -> Result<Vec<Node>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT node_uuid, lifecycle_state FROM indexed_nodes \
             ORDER BY modified_revision DESC, node_uuid LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(params![page.sql_limit(), page.sql_offset()?], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.map(|row| {
            let (bytes, lifecycle) = row?;
            let lifecycle_state = match lifecycle.as_str() {
                "active" => LifecycleState::Active,
                "tombstoned" => LifecycleState::Tombstoned,
                _ => return Err(corrupt("indexed Node lifecycle is invalid")),
            };
            Ok(Node {
                id: decode_node_id(bytes)?,
                lifecycle_state,
            })
        })
        .collect()
    }

    pub(crate) fn list_nodes_cursor(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<Node>, CoreError> {
        const COLLECTION: &str = "index.nodes.modified_revision_desc.uuid_asc";
        let current_revision = self.applied_revision()?;
        let (revision, after) = match cursor {
            Some(cursor) => {
                let payload = decode_cursor(cursor, COLLECTION)?;
                if payload.revision > current_revision {
                    return Err(CoreError::Conflict(
                        "read cursor is stale because the Index revision moved backwards".into(),
                    ));
                }
                if payload.after.len() != 2 {
                    return Err(CoreError::Conflict(
                        "invalid or unsupported read cursor".into(),
                    ));
                }
                let modified_revision = payload.after[0].parse::<u64>().map_err(|_| {
                    CoreError::Conflict("invalid or unsupported read cursor".into())
                })?;
                if i64::try_from(payload.revision).is_err()
                    || i64::try_from(modified_revision).is_err()
                    || modified_revision == 0
                    || modified_revision > payload.revision
                {
                    return Err(CoreError::Conflict(
                        "read cursor revisions are outside the traversal range".into(),
                    ));
                }
                let node_id = payload.after[1].parse::<NodeId>().map_err(|_| {
                    CoreError::Conflict("invalid or unsupported read cursor".into())
                })?;
                (payload.revision, Some((modified_revision, node_id)))
            }
            None => (current_revision, None),
        };

        let limit = bounded_limit(limit);
        let fetch_limit = (limit + 1) as i64;
        let mut rows_with_keys = Vec::new();
        if let Some((after_revision, after_id)) = after {
            let mut statement = self.connection()?.prepare(
                "SELECT node_uuid, lifecycle_state, modified_revision FROM indexed_nodes \
                 WHERE modified_revision <= ?1 \
                   AND (modified_revision < ?2 OR (modified_revision = ?2 AND node_uuid > ?3)) \
                 ORDER BY modified_revision DESC, node_uuid ASC LIMIT ?4",
            )?;
            let rows = statement.query_map(
                params![
                    revision as i64,
                    after_revision as i64,
                    after_id.as_bytes().as_slice(),
                    fetch_limit
                ],
                read_cursor_node,
            )?;
            for row in rows {
                rows_with_keys.push(decode_cursor_node(row?)?);
            }
        } else {
            let mut statement = self.connection()?.prepare(
                "SELECT node_uuid, lifecycle_state, modified_revision FROM indexed_nodes \
                 WHERE modified_revision <= ?1 \
                 ORDER BY modified_revision DESC, node_uuid ASC LIMIT ?2",
            )?;
            let rows =
                statement.query_map(params![revision as i64, fetch_limit], read_cursor_node)?;
            for row in rows {
                rows_with_keys.push(decode_cursor_node(row?)?);
            }
        }

        let has_more = rows_with_keys.len() > limit;
        rows_with_keys.truncate(limit);
        let next_cursor = if has_more {
            let (node, modified_revision) = rows_with_keys
                .last()
                .expect("a page with a continuation contains at least one row");
            Some(encode_cursor(
                COLLECTION,
                revision,
                vec![modified_revision.to_string(), node.id.to_string()],
            )?)
        } else {
            None
        };
        Ok(CursorPage {
            items: rows_with_keys.into_iter().map(|(node, _)| node).collect(),
            next_cursor,
        })
    }

    pub(crate) fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, CoreError> {
        let Some(query) = fts_query(query) else {
            return Ok(Vec::new());
        };
        let limit = limit.clamp(1, 100);
        let mut statement = self.connection()?.prepare(
            "SELECT f.node_uuid, d.title, d.source_kind,
                    snippet(indexed_search_fts, 3, '[', ']', ' … ', 18),
                    bm25(indexed_search_fts, 6.0, 1.0, 0.35)
             FROM indexed_search_fts f
             JOIN indexed_search_documents d ON d.document_key=f.document_key
             JOIN indexed_nodes n ON n.node_uuid=d.node_uuid
             WHERE indexed_search_fts MATCH ?1 AND n.lifecycle_state='active'
             ORDER BY 5, d.document_key
             LIMIT ?2",
        )?;
        let rows = statement.query_map(params![query, (limit * 4) as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
            ))
        })?;
        let mut seen = BTreeSet::new();
        let mut hits = Vec::new();
        for row in rows {
            let (node_id, title, source_kind, snippet, rank) = row?;
            let node_id: NodeId = node_id.parse()?;
            if seen.insert(node_id) {
                hits.push(SearchHit {
                    node_id,
                    title: if title.trim().is_empty() {
                        node_id.to_string()
                    } else {
                        title
                    },
                    source_kind,
                    snippet,
                    rank,
                });
                if hits.len() == limit {
                    break;
                }
            }
        }
        Ok(hits)
    }

    pub(crate) fn backlinks(&self, node_id: NodeId) -> Result<Vec<IndexedRelationship>, CoreError> {
        let mut relationships = Vec::new();
        let mut canonical = self.connection()?.prepare(
            "SELECT r.source_uuid, r.relationship_key, d.inverse_label
             FROM indexed_relationships r
             LEFT JOIN indexed_relationship_definitions d ON d.key=r.relationship_key
             JOIN indexed_nodes source ON source.node_uuid=r.source_uuid
             WHERE r.target_uuid=?1 AND source.lifecycle_state='active'
             ORDER BY r.relationship_key, r.source_uuid",
        )?;
        let rows = canonical.query_map([node_id.as_bytes().as_slice()], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (source, relationship_key, inverse_label) = row?;
            relationships.push(IndexedRelationship {
                source: decode_node_id(source)?,
                relationship_key,
                target: Some(node_id),
                unresolved_target: None,
                provenance: RelationshipProvenance::CanonicalRecord,
                inverse_label,
                source_kind: None,
                source_locator: None,
                parser: None,
                start_offset: None,
                end_offset: None,
            });
        }

        let mut derived = self.connection()?.prepare(
            "SELECT r.source_uuid, r.relationship_key, r.source_kind, r.source_locator,
                    r.parser, r.start_offset, r.end_offset
             FROM indexed_derived_relationships r
             JOIN indexed_nodes source ON source.node_uuid=r.source_uuid
             WHERE r.target_uuid=?1 AND source.lifecycle_state='active'
             ORDER BY r.relationship_key, r.source_uuid, r.start_offset",
        )?;
        let rows = derived.query_map([node_id.as_bytes().as_slice()], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;
        for row in rows {
            let (source, relationship_key, source_kind, source_locator, parser, start, end) = row?;
            relationships.push(IndexedRelationship {
                source: decode_node_id(source)?,
                relationship_key,
                target: Some(node_id),
                unresolved_target: None,
                provenance: RelationshipProvenance::ContentDerived,
                inverse_label: Some("linked from".into()),
                source_kind: Some(source_kind),
                source_locator,
                parser: Some(parser),
                start_offset: Some(
                    usize::try_from(start).map_err(|_| corrupt("negative link offset"))?,
                ),
                end_offset: Some(
                    usize::try_from(end).map_err(|_| corrupt("negative link offset"))?,
                ),
            });
        }
        Ok(relationships)
    }

    pub(crate) fn backlinks_page(
        &self,
        node_id: NodeId,
        page: ReadPage,
    ) -> Result<Vec<IndexedRelationship>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT source_uuid, relationship_key, provenance, inverse_label, source_kind, \
                    source_locator, parser, start_offset, end_offset \
             FROM ( \
                 SELECT r.source_uuid, r.relationship_key, 0 AS provenance, \
                        d.inverse_label, NULL AS source_kind, NULL AS source_locator, \
                        NULL AS parser, NULL AS start_offset, NULL AS end_offset \
                 FROM indexed_relationships r \
                 LEFT JOIN indexed_relationship_definitions d ON d.key=r.relationship_key \
                 JOIN indexed_nodes source ON source.node_uuid=r.source_uuid \
                 WHERE r.target_uuid=?1 AND source.lifecycle_state='active' \
                 UNION ALL \
                 SELECT r.source_uuid, r.relationship_key, 1 AS provenance, \
                        'linked from' AS inverse_label, r.source_kind, r.source_locator, \
                        r.parser, r.start_offset, r.end_offset \
                 FROM indexed_derived_relationships r \
                 JOIN indexed_nodes source ON source.node_uuid=r.source_uuid \
                 WHERE r.target_uuid=?1 AND source.lifecycle_state='active' \
             ) ORDER BY relationship_key, source_uuid, provenance, start_offset \
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = statement.query_map(
            params![
                node_id.as_bytes().as_slice(),
                page.sql_limit(),
                page.sql_offset()?
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                ))
            },
        )?;
        rows.map(|row| {
            let (source, key, provenance, inverse, kind, locator, parser, start, end) = row?;
            Ok(IndexedRelationship {
                source: decode_node_id(source)?,
                relationship_key: key,
                target: Some(node_id),
                unresolved_target: None,
                provenance: if provenance == 0 {
                    RelationshipProvenance::CanonicalRecord
                } else {
                    RelationshipProvenance::ContentDerived
                },
                inverse_label: inverse,
                source_kind: kind,
                source_locator: locator,
                parser,
                start_offset: start
                    .map(|value| {
                        usize::try_from(value).map_err(|_| corrupt("negative link offset"))
                    })
                    .transpose()?,
                end_offset: end
                    .map(|value| {
                        usize::try_from(value).map_err(|_| corrupt("negative link offset"))
                    })
                    .transpose()?,
            })
        })
        .collect()
    }

    pub(crate) fn graph(&self) -> Result<GraphProjection, CoreError> {
        let node_titles = self.active_node_titles()?;
        let mut edges = Vec::new();
        let mut connected = BTreeSet::new();

        let mut canonical = self.connection()?.prepare(
            "SELECT r.source_uuid, r.relationship_key, r.target_uuid
             FROM indexed_relationships r
             JOIN indexed_nodes source ON source.node_uuid=r.source_uuid
             JOIN indexed_nodes target ON target.node_uuid=r.target_uuid
             WHERE source.lifecycle_state='active' AND target.lifecycle_state='active'
             ORDER BY r.source_uuid, r.relationship_key, r.target_uuid",
        )?;
        let rows = canonical.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        for row in rows {
            let (source, relationship_key, target) = row?;
            let source = decode_node_id(source)?;
            let target = decode_node_id(target)?;
            connected.insert(source);
            connected.insert(target);
            edges.push(GraphEdge {
                source,
                relationship_key,
                target: Some(target),
                unresolved_target: None,
                provenance: RelationshipProvenance::CanonicalRecord,
            });
        }

        let mut derived = self.connection()?.prepare(
            "SELECT r.source_uuid, r.relationship_key, r.target_uuid, r.target_text
             FROM indexed_derived_relationships r
             JOIN indexed_nodes source ON source.node_uuid=r.source_uuid
             LEFT JOIN indexed_nodes target ON target.node_uuid=r.target_uuid
             WHERE source.lifecycle_state='active'
               AND (r.target_uuid IS NULL OR target.lifecycle_state='active')
             ORDER BY r.source_uuid, r.relationship_key, r.start_offset",
        )?;
        let rows = derived.query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (source, relationship_key, target, target_text) = row?;
            let source = decode_node_id(source)?;
            let target = target.map(decode_node_id).transpose()?;
            if let Some(target) = target {
                connected.insert(source);
                connected.insert(target);
            }
            edges.push(GraphEdge {
                source,
                relationship_key,
                target,
                unresolved_target: target.is_none().then_some(target_text),
                provenance: RelationshipProvenance::ContentDerived,
            });
        }

        let nodes = node_titles
            .into_iter()
            .map(|(node_id, title)| GraphNode {
                node_id,
                title,
                is_orphan: !connected.contains(&node_id),
            })
            .collect();
        Ok(GraphProjection { nodes, edges })
    }

    pub(crate) fn graph_nodes_page(&self, page: ReadPage) -> Result<Vec<GraphNode>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT n.node_uuid, \
                    COALESCE( \
                        NULLIF((SELECT p.text_value FROM indexed_properties p \
                                WHERE p.node_uuid=n.node_uuid AND p.property_key='core.title'), ''), \
                        NULLIF((SELECT d.title FROM indexed_search_documents d \
                                WHERE d.node_uuid=n.node_uuid AND d.title <> '' \
                                ORDER BY CASE d.source_kind WHEN 'page' THEN 0 WHEN 'file' THEN 1 ELSE 2 END \
                                LIMIT 1), ''), \
                        lower(hex(n.node_uuid)) \
                    ), \
                    NOT EXISTS (SELECT 1 FROM indexed_relationships r \
                                WHERE r.source_uuid=n.node_uuid OR r.target_uuid=n.node_uuid) \
                    AND NOT EXISTS (SELECT 1 FROM indexed_derived_relationships r \
                                    WHERE r.source_uuid=n.node_uuid OR r.target_uuid=n.node_uuid) \
             FROM indexed_nodes n WHERE n.lifecycle_state='active' \
             ORDER BY n.node_uuid LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(params![page.sql_limit(), page.sql_offset()?], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (node_id, title, is_orphan) = row?;
            Ok(GraphNode {
                node_id: decode_node_id(node_id)?,
                title,
                is_orphan,
            })
        })
        .collect()
    }

    pub(crate) fn graph_edges_page(&self, page: ReadPage) -> Result<Vec<GraphEdge>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT source_uuid, relationship_key, target_uuid, unresolved_target, provenance \
             FROM ( \
                 SELECT r.source_uuid, r.relationship_key, r.target_uuid, \
                        NULL AS unresolved_target, 0 AS provenance, 0 AS edge_order \
                 FROM indexed_relationships r \
                 JOIN indexed_nodes source ON source.node_uuid=r.source_uuid \
                 JOIN indexed_nodes target ON target.node_uuid=r.target_uuid \
                 WHERE source.lifecycle_state='active' AND target.lifecycle_state='active' \
                 UNION ALL \
                 SELECT r.source_uuid, r.relationship_key, r.target_uuid, \
                        CASE WHEN r.target_uuid IS NULL THEN r.target_text ELSE NULL END, \
                        1 AS provenance, r.start_offset AS edge_order \
                 FROM indexed_derived_relationships r \
                 JOIN indexed_nodes source ON source.node_uuid=r.source_uuid \
                 LEFT JOIN indexed_nodes target ON target.node_uuid=r.target_uuid \
                 WHERE source.lifecycle_state='active' \
                   AND (r.target_uuid IS NULL OR target.lifecycle_state='active') \
             ) ORDER BY provenance, source_uuid, relationship_key, target_uuid, unresolved_target, edge_order \
             LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(params![page.sql_limit(), page.sql_offset()?], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (source, key, target, unresolved, provenance) = row?;
            Ok(GraphEdge {
                source: decode_node_id(source)?,
                relationship_key: key,
                target: target.map(decode_node_id).transpose()?,
                unresolved_target: unresolved,
                provenance: if provenance == 0 {
                    RelationshipProvenance::CanonicalRecord
                } else {
                    RelationshipProvenance::ContentDerived
                },
            })
        })
        .collect()
    }

    pub(crate) fn orphan_node_ids(&self) -> Result<Vec<NodeId>, CoreError> {
        Ok(self
            .graph()?
            .nodes
            .into_iter()
            .filter(|node| node.is_orphan)
            .map(|node| node.node_id)
            .collect())
    }

    pub(crate) fn orphan_node_count(&self) -> Result<u64, CoreError> {
        self.connection()?
            .query_row(
                "SELECT COUNT(*)
                 FROM indexed_nodes n
                 WHERE n.lifecycle_state='active'
                   AND NOT EXISTS (
                       SELECT 1 FROM indexed_relationships r
                       WHERE r.source_uuid=n.node_uuid OR r.target_uuid=n.node_uuid
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM indexed_derived_relationships r
                       WHERE r.source_uuid=n.node_uuid OR r.target_uuid=n.node_uuid
                   )",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|value| value as u64)
            .map_err(CoreError::from)
    }

    pub(crate) fn orphan_node_ids_limited(&self, limit: usize) -> Result<Vec<NodeId>, CoreError> {
        self.orphan_node_ids_page(ReadPage::new(0, limit))
    }

    pub(crate) fn orphan_node_ids_page(&self, page: ReadPage) -> Result<Vec<NodeId>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT n.node_uuid
             FROM indexed_nodes n
             WHERE n.lifecycle_state='active'
               AND NOT EXISTS (
                   SELECT 1 FROM indexed_relationships r
                   WHERE r.source_uuid=n.node_uuid OR r.target_uuid=n.node_uuid
               )
               AND NOT EXISTS (
                   SELECT 1 FROM indexed_derived_relationships r
                   WHERE r.source_uuid=n.node_uuid OR r.target_uuid=n.node_uuid
               )
             ORDER BY n.node_uuid
             LIMIT ?1 OFFSET ?2",
        )?;
        let rows = statement.query_map(params![page.sql_limit(), page.sql_offset()?], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        rows.map(|row| decode_node_id(row?)).collect()
    }

    fn active_node_titles(&self) -> Result<BTreeMap<NodeId, String>, CoreError> {
        let mut statement = self.connection()?.prepare(
            "SELECT n.node_uuid,
                    COALESCE(
                        NULLIF((SELECT p.text_value FROM indexed_properties p
                                WHERE p.node_uuid=n.node_uuid AND p.property_key='core.title'), ''),
                        NULLIF((SELECT d.title FROM indexed_search_documents d
                                WHERE d.node_uuid=n.node_uuid AND d.title <> ''
                                ORDER BY CASE d.source_kind WHEN 'page' THEN 0 WHEN 'file' THEN 1 ELSE 2 END
                                LIMIT 1), ''),
                        lower(hex(n.node_uuid))
                    )
             FROM indexed_nodes n
             WHERE n.lifecycle_state='active'
             ORDER BY 2 COLLATE NOCASE, n.node_uuid",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut titles = BTreeMap::new();
        for row in rows {
            let (node_id, title) = row?;
            titles.insert(decode_node_id(node_id)?, title);
        }
        Ok(titles)
    }
}

fn read_cursor_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<(Vec<u8>, String, i64)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}

fn decode_cursor_node(
    (bytes, lifecycle, modified_revision): (Vec<u8>, String, i64),
) -> Result<(Node, u64), CoreError> {
    let lifecycle_state = match lifecycle.as_str() {
        "active" => LifecycleState::Active,
        "tombstoned" => LifecycleState::Tombstoned,
        _ => return Err(corrupt("indexed Node lifecycle is invalid")),
    };
    let modified_revision = u64::try_from(modified_revision)
        .map_err(|_| corrupt("indexed Node revision is invalid"))?;
    Ok((
        Node {
            id: decode_node_id(bytes)?,
            lifecycle_state,
        },
        modified_revision,
    ))
}

fn fts_query(value: &str) -> Option<String> {
    let tokens = value
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .take(12)
        .map(|token| format!("\"{token}\"*"))
        .collect::<Vec<_>>();
    (!tokens.is_empty()).then(|| tokens.join(" AND "))
}

fn property_filter_parts(
    filter: &PropertyFilter,
) -> Result<(&'static str, &'static str, &'static str, Option<Value>), CoreError> {
    let (operator, value) = match filter {
        PropertyFilter::Equals(value) => ("=", value),
        PropertyFilter::AtLeast(value) => (">=", value),
        PropertyFilter::AtMost(value) => ("<=", value),
    };
    match value {
        PropertyValue::Null => {
            if operator != "=" {
                return Err(CoreError::Conflict(
                    "null Properties support equality filtering only".into(),
                ));
            }
            Ok(("null", "text_value", operator, None))
        }
        PropertyValue::Boolean(value) => Ok((
            "boolean",
            "integer_value",
            operator,
            Some(Value::Integer(i64::from(*value))),
        )),
        PropertyValue::Integer(value) => Ok((
            "integer",
            "integer_value",
            operator,
            Some(Value::Integer(*value)),
        )),
        PropertyValue::Real(value) => {
            Ok(("real", "real_value", operator, Some(Value::Real(*value))))
        }
        PropertyValue::Text(value) => Ok((
            "text",
            "text_value",
            operator,
            Some(Value::Text(value.clone())),
        )),
        PropertyValue::Date(value) => Ok((
            "date",
            "text_value",
            operator,
            Some(Value::Text(value.clone())),
        )),
        PropertyValue::DateTime(value) => Ok((
            "date_time",
            "text_value",
            operator,
            Some(Value::Text(value.clone())),
        )),
        PropertyValue::List(values) => {
            if operator != "=" {
                return Err(CoreError::Conflict(
                    "list Properties support equality filtering only".into(),
                ));
            }
            let encoded = PropertyValue::List(values.clone())
                .canonical_json()
                .ok_or_else(|| {
                    CoreError::InvalidPropertyValue("query list value could not be encoded".into())
                })?;
            Ok(("list", "json_value", operator, Some(Value::Text(encoded))))
        }
        PropertyValue::Structured(value) => {
            if operator != "=" {
                return Err(CoreError::Conflict(
                    "structured Properties support equality filtering only".into(),
                ));
            }
            let encoded = PropertyValue::Structured(value.clone())
                .canonical_json()
                .ok_or_else(|| {
                    CoreError::InvalidPropertyValue(
                        "query structured value could not be encoded".into(),
                    )
                })?;
            Ok((
                "structured",
                "json_value",
                operator,
                Some(Value::Text(encoded)),
            ))
        }
    }
}

fn decode_indexed_value(
    kind: &str,
    text: Option<String>,
    integer: Option<i64>,
    real: Option<f64>,
    json: Option<String>,
) -> Result<PropertyValue, CoreError> {
    match kind {
        "null" => Ok(PropertyValue::Null),
        "boolean" => match integer {
            Some(0) => Ok(PropertyValue::Boolean(false)),
            Some(1) => Ok(PropertyValue::Boolean(true)),
            _ => Err(corrupt("indexed boolean Property is invalid")),
        },
        "integer" => integer
            .map(PropertyValue::Integer)
            .ok_or_else(|| corrupt("indexed integer Property is missing its value")),
        "real" => real
            .map(PropertyValue::Real)
            .ok_or_else(|| corrupt("indexed real Property is missing its value")),
        "text" => text
            .map(PropertyValue::Text)
            .ok_or_else(|| corrupt("indexed text Property is missing its value")),
        "date" => text
            .map(PropertyValue::Date)
            .ok_or_else(|| corrupt("indexed date Property is missing its value")),
        "date_time" => text
            .map(PropertyValue::DateTime)
            .ok_or_else(|| corrupt("indexed date-time Property is missing its value")),
        "list" => json
            .ok_or_else(|| corrupt("indexed list Property is missing its JSON value"))
            .and_then(|value| {
                serde_json::from_str(&value)
                    .map(PropertyValue::List)
                    .map_err(|_| corrupt("indexed list Property JSON is invalid"))
            }),
        "structured" => json
            .ok_or_else(|| corrupt("indexed structured Property is missing its JSON value"))
            .and_then(|value| {
                serde_json::from_str(&value)
                    .map(PropertyValue::Structured)
                    .map_err(|_| corrupt("indexed structured Property JSON is invalid"))
            }),
        _ => Err(corrupt("indexed Property kind is invalid")),
    }
}

fn decode_node_id(bytes: Vec<u8>) -> Result<NodeId, CoreError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| corrupt("indexed Node UUID is not 16 bytes"))?;
    NodeId::from_bytes(bytes)
}

fn corrupt(reason: &str) -> CoreError {
    CoreError::CorruptStore {
        store: "Index Store",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::fts_query;

    #[test]
    fn fts_input_is_tokenized_instead_of_interpreted_as_query_syntax() {
        assert_eq!(
            fts_query("Genesis OR title:*"),
            Some("\"Genesis\"* AND \"OR\"* AND \"title\"*".into())
        );
        assert_eq!(fts_query("***"), None);
    }
}
