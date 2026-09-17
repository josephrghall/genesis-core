# Architecture evolution

Genesis Core evolved through technical separation rather than a redesign:

~~~text
embedded information layer
        ↓
stable identity separated from representation
        ↓
generic Properties and Relationships
        ↓
Record established as canonical
        ↓
Index established as derived
        ↓
six-primitive boundary established
        ↓
headless independence proven
        ↓
durability and recovery hardened
        ↓
neutral real-content validation
        ↓
heterogeneous/topology validation
        ↓
Developer Preview
~~~

The result is one continuous architecture. The extraction did not add a primitive or create a separate “v2.” It removed product assumptions, made authority and lifecycle semantics explicit, and proved the engine could build and validate without a parent application.

Pre-extraction granular history remains in its original private historical source. This repository begins with the curated standalone Core project so deleted or unrelated private material cannot appear in public history.
