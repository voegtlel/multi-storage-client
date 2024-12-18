# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict

from jsonschema import validate


OTEL_SCHEMA = {
    "type": "object",
    "properties": {
        "metrics": {
            "type": "object",
            "properties": {
                "exporter": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["otlp", "console"],
                        },
                        "options": {
                            "type": "object",
                            "properties": {
                                "endpoint": {"type": "string", "format": "uri"},
                            },
                            "required": ["endpoint"],
                        },
                    },
                    "required": ["type"],
                }
            },
        },
        "traces": {
            "type": "object",
            "properties": {
                "exporter": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["otlp", "console"],
                        },
                        "options": {
                            "type": "object",
                            "properties": {
                                "endpoint": {"type": "string", "format": "uri"},
                            },
                            "required": ["endpoint"],
                        },
                    },
                    "required": ["type"],
                }
            },
        },
    },
    "additionalProperties": False,
}

CACHE_SCHEMA = {
    "type": "object",
    "properties": {
        "location": {"type": "string"},
        "use_etag": {"type": "boolean"},
        "size_mb": {"type": "integer"},
    },
    "additionalProperties": False,
}

EXTENSION_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "options": {
            "type": "object",
        },
    },
    "required": ["type"],
}

PROFILE_SCHEMA = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "properties": {
            "storage_provider": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["file", "s3", "oci", "gcs", "azure", "ais", "s8k"],
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "base_path": {"type": "string", "minLength": 0},
                        },
                        "required": ["base_path"],
                    },
                },
                "required": ["type", "options"],
            },
            "credentials_provider": EXTENSION_SCHEMA,
            "metadata_provider": EXTENSION_SCHEMA,
            "provider_bundle": EXTENSION_SCHEMA,
            "comment": {"type": "string"},
        },
        "oneOf": [
            {
                "required": ["storage_provider"],
            },
            {
                "required": ["provider_bundle"],
            },
        ],
    },
}

CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "profiles": PROFILE_SCHEMA,
        "cache": CACHE_SCHEMA,
        "opentelemetry": OTEL_SCHEMA,
        "additionalProperties": False,
    },
    "required": ["profiles"],
}


def validate_config(config_dict: Dict[str, Any]) -> None:
    try:
        validate(instance=config_dict, schema=CONFIG_SCHEMA)
    except Exception as e:
        raise RuntimeError("Failed to validate the config file") from e
