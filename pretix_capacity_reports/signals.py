# Register your receivers here
from django.dispatch import receiver

from pretix.base.signals import register_multievent_data_exporters, register_data_exporters


@receiver(register_multievent_data_exporters, dispatch_uid="capacity_reports_export1_multi")
@receiver(register_data_exporters, dispatch_uid="capacity_reports_export1")
def register_export1(sender, **kwargs):
    from .exporter import CapacityUtilizationReport
    return CapacityUtilizationReport


@receiver(register_multievent_data_exporters, dispatch_uid="capacity_reports_export2_multi")
def register_export2(sender, **kwargs):
    from .exporter import CapacityCreationReport
    return CapacityCreationReport
