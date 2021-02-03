from django.utils.translation import gettext_lazy

try:
    from pretix.base.plugins import PluginConfig
except ImportError:
    raise RuntimeError("Please use pretix 2.7 or above to run this plugin!")

__version__ = "1.0.0"


class PluginApp(PluginConfig):
    name = "pretix_capacity_reports"
    verbose_name = "Capacity reporting"

    class PretixPluginMeta:
        name = gettext_lazy("Capacity reporting")
        author = "pretix team"
        description = gettext_lazy("Capacity and utilization reports")
        visible = True
        version = __version__
        category = "FORMAT"
        compatibility = "pretix>=3.14.0"

    def ready(self):
        from . import signals  # NOQA


default_app_config = "pretix_capacity_reports.PluginApp"
