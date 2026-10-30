# Local Airflow compatibility

The adapters in this directory are installed in ../airflow_runtime_compat,
which compose.yaml already includes in the container PYTHONPATH.
No utils source files are changed.

conjur_api resolves secret keys from environment variables (optional env: prefix).
It only accepts local://environment; it does not contact a Conjur server.
sitecustomize supplies local CONJUR_* defaults and aliases the legacy situtils
ConfigClient and SSHOperatorOcbc imports to existing implementations.
local.token is a non-secret marker used only by this local adapter.

DAG import validation does not execute tasks or validate remote connections.
