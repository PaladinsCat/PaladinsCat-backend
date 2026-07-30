# PaladinsCat shared Rust core

Shared domain and infrastructure code for the native API, worker, and operator
binaries belongs here. HirezRelay remains a separate security and quota
boundary.

The first implemented compatibility surface is typed configuration. Secret
values such as database and Redis URLs are deliberately omitted from serialized
status output. Defaults, bounds, and legacy aliases have executable tests.
