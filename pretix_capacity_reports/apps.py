from django.utils.translation import gettext_lazy

from . import __version__

try:
    from pretix.base.plugins import PluginConfig
except ImportError:
    raise RuntimeError("Please use pretix 2.7 or above to run this plugin!")


class PluginApp(PluginConfig):
    default = True
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


