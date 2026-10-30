"""Compatibility imports for the local Airflow container; source utils stays intact."""
import importlib.abc
import importlib.util
import os
import sys

os.environ.setdefault('CONJUR_APPLIANCE_URL', 'local://environment')
os.environ.setdefault('CONJUR_ACCOUNT', 'local')
os.environ.setdefault('CONJUR_AUTHN_LOGIN', 'local')
os.environ.setdefault('CONJUR_AUTHN_TOKEN_FILE', '/opt/airflow/dags/local_runtime/local.token')

class LocalImports(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    packages = {'dags.utils.situtils', 'dags.utils.operators', 'dags.utils.operators.ssh'}
    aliases = {
        'dags.utils.situtils.BaseDagsClient': 'dags.utils.BaseDags.BaseDagsClient',
        'dags.utils.operators.ssh.ssh_operator': 'airflow.providers.ssh.operators.ssh',
    }

    def find_spec(self, fullname, path=None, target=None):
        if fullname in self.packages or fullname in self.aliases:
            return importlib.util.spec_from_loader(fullname, self, is_package=fullname in self.packages)

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        if module.__name__ in self.packages:
            return
        from importlib import import_module
        original = import_module(self.aliases[module.__name__])
        if module.__name__.endswith('ssh_operator'):
            module.SSHOperatorOcbc = original.SSHOperator
        else:
            for name in dir(original):
                if not name.startswith('_'):
                    setattr(module, name, getattr(original, name))

sys.meta_path.insert(0, LocalImports())
