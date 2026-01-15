from bec_messages import ScanQueueMessage as _ScanQueueMessage
from bec_messages import (
    ProcedureWorkerStatus,
    BECStatus,
    BECMessage,
    BundleMessage,
    ScanQueueHistoryMessage,
    ScanStatusMessage,
    ScanQueueModificationMessage,
    ScanQueueOrderMessage,
    RequestBlock,
    QueueInfoEntry,
    ScanQueueStatus,
    ScanQueueStatusMessage,
    ClientInfoMessage,
    RequestResponseMessage,
    DeviceInstructionMessage,
    ErrorInfo,
    DeviceInstructionResponse,
    DeviceMessage,
    DeviceAsyncUpdate,
    DeviceRPCMessage,
    DeviceStatusMessage,
    DeviceReqStatusMessage,
    DeviceInfoMessage,
    DeviceMonitor2DMessage,
    DeviceMonitor1DMessage,
    DevicePreviewMessage,
    DeviceUserROIMessage,
    ScanMessage,
    ScanHistoryMessage,
    ScanBaselineMessage,
    DeviceConfigMessage,
    DeviceInitializationProgressMessage,
    LogMessage,
    AlarmMessage,
    StatusMessage,
    FileMessage,
    FileContentMessage,
    VariableMessage,
    ObserverMessage,
    ServiceMetricMessage,
    ProcessedDataMessage,
    DAPConfigMessage,
    DAPRequestMessage,
    DAPResponseMessage,
    AvailableResourceMessage,
    ProgressMessage,
    GUIConfigMessage,
    GUIDataMessage,
    GUIInstructionMessage,
    GUIAutoUpdateConfigMessage,
    GUIRegistryStateMessage,
    ServiceResponseMessage,
    CredentialsMessage,
    RawMessage,
    ServiceRequestMessage,
    ProcedureRequestMessage,
    ProcedureQNotifMessage,
    ProcedureStatusUpdate,
    ProcedureExecutionMessage,
    ProcedureAbortMessage,
    ProcedureClearUnhandledMessage,
    ProcedureWorkerStatusMessage,
    LoginInfoMessage,
    ACLAccountsMessage,
    EndpointInfoMessage,
    ScriptExecutionInfoMessage,
    MacroUpdateMessage,
)

from bec_lib.metadata_schema import get_metadata_schema_for_scan
from pydantic import model_validator, ValidationError


class ScanQueueMessage(_ScanQueueMessage):
    @model_validator(mode="after")
    @classmethod
    def _validate_metadata(cls, data):
        """Make sure the metadata conforms to the registered schema, but leave it as a dict"""
        schema = get_metadata_schema_for_scan(data.scan_type)
        try:
            schema.model_validate(data.metadata.get("user_metadata", {}))
        except ValidationError as e:
            raise ValueError(
                f"Scan metadata {data.metadata} does not conform to registered schema {schema}. \n Errors: {str(e)}"
            ) from e
        return data
