INSERT INTO tfos_admin.config (key, value) VALUES (
'upstream', '
{
  "upstreams": [
    {
      "hostSet": [
        "https://172.18.0.14:8443",
        "https://172.18.0.12:8443"
      ],
      "operationSet": {
        "operation": [
          {
            "operationPolicy": "UNDERFAILURE",
            "operationType": "ADMIN_READ"
          },
          {
            "operationPolicy": "ALWAYS",
            "operationType": "ADMIN_WRITE"
          }
        ]
      }
    },
    {
      "hostSet": [
        "https://172.18.0.16:8443"
      ],
      "operationSet": {
        "operation": [
          {
            "operationPolicy": "UNDERFAILURE",
            "operationType": "EXTRACT"
          },
          {
            "operationPolicy": "UNDERFAILURE",
            "operationType": "INGEST"
          },
          {
            "operationPolicy": "UNDERFAILURE",
            "operationType": "CONTEXT_WRITE"
          }
        ]
      }
    },
    {
      "hostSet": [
        "https://172.18.0.11:8443",
        "https://172.18.0.12:8443",
        "https://172.18.0.10:8443"
      ],
      "operationSet": {
        "operation": [
          {
            "operationPolicy": "ALWAYS",
            "operationType": "INGEST"
          },
          {
            "operationPolicy": "ALWAYS",
            "operationType": "CONTEXT_WRITE"
          },
          {
            "operationPolicy": "ALWAYS",
            "operationType": "EXTRACT"
          },
          {
            "operationPolicy": "ALWAYS",
            "operationType": "SUMMARIZE"
          },
          {
            "operationPolicy": "ALWAYS",
            "operationType": "ADMIN_READ"
          }
        ]
      }
    }
  ],
  "lbPolicy": "ROUND_ROBIN"
}');
