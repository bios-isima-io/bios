
export const TenantAppendixCategory = {
  IMPORT_SOURCES: 'IMPORT_SOURCES',
  IMPORT_DESTINATIONS: 'IMPORT_DESTINATIONS',
  IMPORT_FLOW_SPECS: 'IMPORT_FLOW_SPECS',
  IMPORT_DATA_PROCESSORS: 'IMPORT_DATA_PROCESSORS',
  EXPORT_TARGETS: 'EXPORT_TARGETS',
};

export const ImportDataMappingType = {
  JSON: 'Json',
  CSV: 'Csv'
};

export const ImportSourceType = {
  WEBHOOK: "Webhook",
  FILE: "File",
  HIBERNATE: "Hibernate",
  KAFKA: "Kafka",
};

export const ImportDestinationType = {
  BIOS: "Bios"
}

export const IntegrationsAuthenticationType = {
  LOGIN: "Login",
  SASL_PLAINTEXT: "SaslPlaintext",
  IN_MESSAGE: "InMessage",
};
