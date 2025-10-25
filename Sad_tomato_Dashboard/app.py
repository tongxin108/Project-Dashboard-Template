import os
import datetime as dt
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from pymongo import MongoClient

# 启动语句
# streamlit run app.py

load_dotenv()


#Postgres schema helper
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")   # CHANGE: "public" to your own schema name
def qualify(sql: str) -> str:
    # Replace occurrences of {S}.<table> with <schema>.<table>
    return sql.replace("{S}.", f"{PG_SCHEMA}.")

# CONFIG: Postgres and Mongo Queries
CONFIG = {
    "postgres": {
    "enabled": True,
    "uri": os.getenv("PG_URI", "postgresql+psycopg2://postgres:password@localhost:5432/postgres"),
    "queries": {
        # User 1: ELDERLY
        "Elder: Show my current health data including heart rate, blood pressure, and oxygen levels.": {
            "sql": """
            SELECT 
                e.elderly_name,
                v.heart_rate_at_alert AS heart_rate,
                v.blood_pressure_at_alert AS blood_pressure,
                v.oxygen_saturation_at_alert AS oxygen_level,
                v.glucose_level_at_alert AS glucose_level,
                a.alert_timestamp AS last_reading_time
            FROM elderly e
            LEFT JOIN alerts a ON e.elderly_id = a.elderly_id
            LEFT JOIN alert_vitals v ON a.alert_id = v.alert_id
            WHERE e.elderly_id = :elderly_id
            AND a.alert_timestamp IS NOT NULL
            ORDER BY a.alert_timestamp DESC
            LIMIT 1;
            """,
            "chart": {"type": "table"},
            "tags": ["elderly"],
            "params": ["elderly_id"]
        },
        
        "Elder: Display my recent alert history with timestamps and alert types.": {
            "sql": """
            SELECT 
                alert_timestamp,
                alert_type,
                alert_status,
                device_id
            FROM alerts
            WHERE elderly_id = :elderly_id
            AND alert_timestamp IS NOT NULL
            AND alert_timestamp IS NOT NULL
            ORDER BY alert_timestamp DESC;
            """,
            "chart": {"type": "table"},
            "tags": ["elderly"],
            "params": ["elderly_id"]
        },
        
        "elder: List my emergency contacts with names and phone numbers for quick reference.": {
            "sql": """
            SELECT 
                c.contact_name,
                c.contact_phone,
                ec.relationship_type
            FROM elderly_contacts ec
            JOIN contacts c ON ec.contact_id = c.contact_id
            WHERE ec.elderly_id = :elderly_id
            ORDER BY ec.is_primary DESC;
            """,
            "chart": {"type": "table"},
            "tags": ["elderly"],
            "params": ["elderly_id"]
        },

        # User 2: Medical Worker 
        # ok下面的语句
        "Medical Worker: Show all alerts for elderly under my care that require attention.": {
            "sql": """
            SELECT 
                e.elderly_id,
                e.elderly_name,
                a.alert_type,
                a.alert_timestamp,
                a.device_id,
                v.heart_rate_at_alert,
                v.blood_pressure_at_alert,
                v.oxygen_saturation_at_alert
            FROM alerts a
            JOIN elderly e ON a.elderly_id = e.elderly_id
            LEFT JOIN alert_vitals v ON a.alert_id = v.alert_id
            JOIN elderly_medical_workers emw ON e.elderly_id = emw.elderly_id
            WHERE emw.medical_worker_id = :medical_worker_id
            AND a.alert_status = 'triggered'
            
            ORDER BY a.alert_timestamp DESC;
            """,
            "chart": {"type": "table"},
            "tags": ["medical_worker"],  
            "params": ["medical_worker_id"]
        },
       # ok下面的语句
        "Medical Worker: Generate alert reports for specific elderly showing vital sign changes over time.": {
            "sql": """
            SELECT 
                a.alert_timestamp,
                v.heart_rate_at_alert,
                v.blood_pressure_at_alert,
                v.oxygen_saturation_at_alert,
                v.glucose_level_at_alert,
                a.alert_type
            FROM alerts a
            JOIN alert_vitals v ON a.alert_id = v.alert_id
            WHERE a.elderly_id = :elderly_id
            AND a.alert_timestamp IS NOT NULL
            AND v.heart_rate_at_alert IS NOT NULL
            ORDER BY a.alert_timestamp ASC;
            """,
            "chart": {"type": "table"},
            "tags": ["medical_worker"],  
            "params": ["elderly_id"]  
        },
        # ok下面的语句
        "Medical Worker: View detailed elderly profiles including medical conditions and contact information.": {
            "sql": """
            SELECT 
                e.elderly_id,
                e.elderly_name,
                e.elderly_age,
                e.address,
                d.device_id,
                d.device_status,
                STRING_AGG(DISTINCT c.condition_name, ', ') AS medical_conditions,
                STRING_AGG(DISTINCT con.contact_name || ' (' || con.contact_phone || ')', '; ') AS emergency_contacts
            FROM elderly e
            LEFT JOIN elderly_devices ed ON e.elderly_id = ed.elderly_id
            LEFT JOIN devices d ON ed.device_id = d.device_id
            LEFT JOIN elderly_conditions ec ON e.elderly_id = ec.elderly_id
            LEFT JOIN conditions c ON ec.condition_id = c.condition_id
            LEFT JOIN elderly_contacts ecl ON e.elderly_id = ecl.elderly_id
            LEFT JOIN contacts con ON ecl.contact_id = con.contact_id
            WHERE e.elderly_id = :elderly_id
            GROUP BY e.elderly_id, e.elderly_name, e.elderly_age, e.address, d.device_id, d.device_status;
            """,
            "chart": {"type": "table"},
            "tags": ["medical_worker"], 
            "params": ["elderly_id"]
        },

        # User 3: Emergency Contact 
        # ok下
        "Emergency Contact: Receive real-time notifications when linked elderly trigger emergency alerts.": {
            "sql": """
            SELECT 
                e.elderly_name,
                a.alert_type,
                a.alert_timestamp,
                v.heart_rate_at_alert,
                v.blood_pressure_at_alert,
                v.oxygen_saturation_at_alert
            FROM alerts a
            JOIN elderly e ON a.elderly_id = e.elderly_id
            LEFT JOIN alert_vitals v ON a.alert_id = v.alert_id
            JOIN elderly_contacts ec ON e.elderly_id = ec.elderly_id
            WHERE ec.contact_id = :emergency_contact_id
            AND a.alert_status = 'triggered'
            AND a.alert_timestamp IS NOT NULL
            ORDER BY a.alert_timestamp DESC;
            """,
            "chart": {"type": "table"},
            "tags": ["emergency_contact"],  
            "params": ["emergency_contact_id"]
        },
        
        #ok
        "Emergency Contact: Check current status and last known vital signs of connected elderly persons.": {
            "sql": """
            SELECT 
                e.elderly_name,
                e.elderly_age,
                d.device_status,
                a.alert_timestamp AS last_alert_time,
                a.alert_type AS last_alert_type,
                v.heart_rate_at_alert AS last_heart_rate,
                v.blood_pressure_at_alert AS last_blood_pressure,
                v.oxygen_saturation_at_alert AS last_oxygen_level
            FROM elderly e
            LEFT JOIN elderly_devices ed ON e.elderly_id = ed.elderly_id
            LEFT JOIN devices d ON ed.device_id = d.device_id
            LEFT JOIN alerts a ON e.elderly_id = a.elderly_id
            LEFT JOIN alert_vitals v ON a.alert_id = v.alert_id
            WHERE e.elderly_id IN (
                SELECT elderly_id 
                FROM elderly_contacts 
                WHERE contact_id = :emergency_contact_id
            )
            AND a.alert_timestamp IS NOT NULL
            ORDER BY a.alert_timestamp DESC
            LIMIT 5;
            """,
            "chart": {"type": "table"},  
            "tags": ["emergency_contact"],  
            "params": ["emergency_contact_id"]  
        },
        
        "Emergency Contact: Access emergency response protocols and contact numbers for quick action.": {
            "sql": """
            SELECT 
                contact_id,
                contact_name,
                contact_phone,
                'Emergency contact' as role_type
            FROM contacts
            ORDER BY contact_id;
            """,
            "chart": {"type": "table"},
            "tags": ["emergency_contact"],  # 改为小写
            "params": []  # 明确表示没有参数
        },

        # User 4: System Administrator
        "System Administrator: Monitor system-wide device status and identify any inactive or malfunctioning devices.": {
            "sql": """
            SELECT 
                d.device_id,
                d.device_status,
                COUNT(*) as total_assignments,
                (SELECT COUNT(*) FROM alerts WHERE device_id = d.device_id) as total_alerts
            FROM devices d
            LEFT JOIN elderly_devices ed ON d.device_id = ed.device_id
            GROUP BY d.device_id, d.device_status
            ORDER BY 
                CASE d.device_status 
                    WHEN 'inactive' THEN 1
                    WHEN 'maintenance' THEN 2
                    ELSE 3 
                END,
                total_alerts DESC;
            """,
            "chart": {"type": "bar","x": "device_status", "y": ["total_assignments", "total_alerts"],"color": "device_status",},
            "tags": ["system_administrator"],  # 改为小写
            "params": []  # 这个查询不需要参数
        },
        #ok
        "System Administrator: Generate usage statistics showing alert frequency and response times by medical staff.": {
            "sql": """
            SELECT 
                mw.medical_worker_name,
                a.alert_type,
                COUNT(a.alert_id) as alert_count,
                AVG(CASE WHEN a.alert_status = 'resolved' THEN 1 ELSE 0 END) as resolution_rate
            FROM medical_workers mw
            LEFT JOIN elderly_medical_workers emw ON mw.medical_worker_id = emw.medical_worker_id
            LEFT JOIN alerts a ON emw.elderly_id = a.elderly_id
            WHERE a.alert_timestamp IS NOT NULL
            GROUP BY mw.medical_worker_name, a.alert_type
            """,
            "chart": {"type": "heatmap","rows": "medical_worker_name","cols": "alert_type", "values": "alert_count"},
            "tags": ["system_administrator"],  # 改为小写
            
        },
        #ok
        "System Administrator: Calculate a health score for each device": {
            "sql": """
            SELECT 
                d.device_id,
                d.device_status,
                e.elderly_name,
                COUNT(a.alert_id) as total_alerts,
                COUNT(DISTINCT a.alert_type) as alert_variety,
                AVG(av.heart_rate_at_alert) as avg_heart_rate_alert,
                AVG(av.glucose_level_at_alert) as avg_glucose_alert,
                CASE 
                    WHEN COUNT(a.alert_id) = 0 THEN 100
                    WHEN COUNT(a.alert_id) BETWEEN 1 AND 3 THEN 80
                    WHEN COUNT(a.alert_id) BETWEEN 4 AND 10 THEN 60
                    ELSE 40
                END as health_score
            FROM Devices d
            LEFT JOIN Elderly_Devices ed ON d.device_id = ed.device_id
            LEFT JOIN Elderly e ON ed.elderly_id = e.elderly_id
            LEFT JOIN Alerts a ON d.device_id = a.device_id
            LEFT JOIN Alert_Vitals av ON a.alert_id = av.alert_id
            GROUP BY d.device_id, d.device_status, e.elderly_name
            ORDER BY health_score ASC, total_alerts DESC;
            """,
            "chart": {"type": "bar","x": "health_score",  "y": "device_id"},
            "tags": ["system_administrator"],  
            
        }
    }
},

    "mongo": {
        "enabled": True,
        "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),  # Will read from the .env file
        "db_name": os.getenv("MONGO_DB", "eldercare"),               # Will read from the .env file
        
        # CHANGE: Just like above, replace all the following Mongo queries with your own, for the different users you identified
        "queries": {
            "elderly: Heart rate distribution analysis": {
            "collection": "sensor_readings",
            "aggregate": [
                {
                    "$match": {
                        
                        "vital_signs.heart_rate": {"$exists": True, "$ne": None}
                    }
                },
                {
                    "$bucket": {
                        "groupBy": "$vital_signs.heart_rate",
                        "boundaries": [0, 50, 60, 80, 100, 120, 200],
                        "default": "其他",
                        "output": {
                            "count": {"$sum": 1},
                            "avg_heart_rate": {"$avg": "$vital_signs.heart_rate"}
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "heart_rate_range": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$eq": ["$_id", 0]}, "then": "0-50 (过低)"},
                                    {"case": {"$eq": ["$_id", 50]}, "then": "50-60 (偏低)"},
                                    {"case": {"$eq": ["$_id", 60]}, "then": "60-80 (正常)"},
                                    {"case": {"$eq": ["$_id", 80]}, "then": "80-100 (正常)"},
                                    {"case": {"$eq": ["$_id", 100]}, "then": "100-120 (偏高)"},
                                    {"case": {"$eq": ["$_id", 120]}, "then": "120+ (过高)"}
                                ],
                                "default": "其他"
                            }
                        },
                        "reading_count": "$count",
                        "avg_rate": {"$round": ["$avg_heart_rate", 1]}
                    }
                },
                {
                    "$sort": {
                        "reading_count": -1
                    }
                }
            ],
            "chart": {"type": "pie", "names": "heart_rate_range", "values": "reading_count"},
            "tags": ["medical_worker", "admin"]
        },
            "elderly: Query abnormal physiological data statistics (grouped by elderly)": {
                    "collection": "sensor_readings",
                    "aggregate": [
                        {
                            "$match": {
                                "is_abnormal": True
                            }
                        },
                        {
                            "$group": {
                                "_id": "$elderly_id",
                                "total_abnormal_readings": {"$sum": 1},
                                "abnormal_details": {
                                    "$push": {
                                        "timestamp": "$ts",
                                        "heart_rate": "$vital_signs.heart_rate",
                                        "blood_pressure": "$vital_signs.blood_pressure",
                                        "oxygen_level": "$vital_signs.oxygen_level",
                                        "body_temperature": "$vital_signs.body_temperature",
                                        "glucose_level": "$vital_signs.glucose_level"
                                    }
                                }
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "elderly_id": "$_id",
                                "total_abnormal_readings": 1,
                                "latest_abnormal": {"$arrayElemAt": ["$abnormal_details", -1]}
                            }
                        },
                        {
                            "$sort": {
                                "total_abnormal_readings": -1
                            }
                        }
                    ],
                    "chart": {"type": "bar", "x": "elderly_id", "y": "total_abnormal_readings"},
                    "tags": ["medical_worker", "admin"]
            },
            
            "elderly: Query device usage statistics and summary of elderly health data": {
                "collection": "sensor_readings",
                "aggregate": [
                    {
                        "$group": {
                            "_id": {
                                "elderly_id": "$elderly_id",
                                "device_id": "$device_id"
                            },
                            "total_readings": {"$sum": 1},
                            "avg_heart_rate": {"$avg": "$vital_signs.heart_rate"},
                            "avg_oxygen_level": {"$avg": "$vital_signs.oxygen_level"},
                            "avg_body_temperature": {"$avg": "$vital_signs.body_temperature"},
                            "last_reading_time": {"$max": "$ts"}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "elderly_id": "$_id.elderly_id",
                            "device_id": "$_id.device_id",
                            "total_readings": 1,
                            "avg_heart_rate": {"$round": ["$avg_heart_rate", 1]},
                            "avg_oxygen_level": {"$round": ["$avg_oxygen_level", 1]},
                            "avg_body_temperature": {"$round": ["$avg_body_temperature", 1]},
                            "last_reading_time": 1
                        }
                    },
                    {
                        "$sort": {
                            "elderly_id": 1,
                            "total_readings": -1
                        }
                    }
                ],
                "chart": {"type": "bar",  "x": "device_id", "y": "total_readings","color": "elderly_id"},
                "tags": ["medical_worker", "admin", "family_member"]
            },
            
            "Emergency Contact: Alert Notifications": {
            "collection": "alert_readings",
            "aggregate": [
                {
                    "$match": {
                        "contact_ids": "CONTACT_005"
                    }
                },
                {
                    "$sort": {
                        "ts": -1
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "alert_id": 1,
                        "timestamp": "$ts",
                        "alert_type": 1,
                        "alert_status": 1,
                        "elderly_id": 1
                    }
                }
            ],
            "chart": {"type": "table"},
            "tags": ["emergency_contact"]
            },
            
            "Emergency Contact: Pending Alert Tasks": {
            "collection": "alert_readings",
            "aggregate": [
                {
                    "$match": {
                        "contact_ids": {
                            "$in": ["CONTACT_005", "CONTACT_037"]
                        },
                        "alert_status": {
                            "$in": ["pending", "processing"]
                        }
                    }
                },
                {
                    "$sort": {
                        "ts": -1
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "alert_id": 1,
                        "timestamp": "$ts",
                        "alert_type": 1,
                        "elderly_id": 1,
                        "alert_status": 1,
                        "heart_rate": 1,
                        "blood_pressure": 1
                    }
                }
            ],
            "chart": {"type": "table"},
            "tags": ["emergency_contact"]
        },
            
            
            "Emergency Contact: Monthly Alert Summary": {
                "collection": "alert_readings",
                "aggregate": [
                    {
                        "$match": {
                            "contact_ids": "CONTACT_005",
                        }
                    },
                    {
                        "$group": {
                            "_id": "$alert_type",
                            "total_alerts": {"$sum": 1},
                            "resolved_alerts": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$alert_status", "resolved"]},
                                        1,
                                        0
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "alert_type": "$_id",
                            "total_alerts": 1,
                            "resolved_alerts": 1,
                            "resolution_rate": {
                                "$round": [
                                    {"$multiply": [{"$divide": ["$resolved_alerts", "$total_alerts"]}, 100]},
                                    2
                                ]
                            }
                        }
                    }
                ],
                "chart": {"type": "bar","x": "alert_type", "y": "total_alerts","secondary_y": "resolution_rate"},
                "tags": ["emergency_contact"]
            },
            
        "Medical workers: Daily average body temperature trend calculated by group of elderly people": {
            "collection": "sensor_readings",
            "aggregate": [
                {
                    "$group": {
                        "_id": {
                            "elderly_id": "$elderly_id",
                            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}}
                        },
                        "avg_temp": {"$avg": "$vital_signs.body_temperature"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "elderly_id": "$_id.elderly_id",
                        "date": "$_id.date",
                        "avg_temp": {"$round": ["$avg_temp", 1]}
                    }
                },
                {
                    "$sort": {
                        "elderly_id": 1,
                        "date": 1
                    }
                }
            ],
            "chart": {"type": "line","x": "date","y": "avg_temp","color": "elderly_id"},
            "tags": ["medical_worker", "admin"]
        
            },
        "Medical workers: Frequency of alarms at each time period": {
            "collection": "alert_readings",
            "aggregate": [
                
                {
                    "$group": {
                        "_id": {
                            "hour": {"$hour": "$ts"}
                        },
                        "alert_count": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "hour": "$_id.hour",
                        "alert_count": 1
                    }
                },
                {
                    "$sort": {
                        "hour": 1
                    }
                }
            ],
            "chart": {"type": "bar", "x": "hour", "y": "alert_count",},
            "tags": ["medical_worker", "admin"]
        },
            
            "Medical workers: Daily fall alert statistics": {
            "collection": "alert_readings",
            "aggregate": [
                {
                    "$match": {
                        "alert_type": "fall alert"
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}},
                            "elderly_id": "$elderly_id"
                        },
                        "fall_count": {"$sum": 1}
                    }
                },
                {
                    "$group": {
                        "_id": "$_id.date",
                        "total_falls": {"$sum": "$fall_count"},
                        "affected_elders": {"$addToSet": "$_id.elderly_id"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "date": "$_id",
                        "total_falls": 1,
                        "affected_elders_count": {"$size": "$affected_elders"}
                    }
                },
                {
                    "$sort": {
                        "date": 1
                    }
                }
            ],
            "chart": {"type": "bar", "x": "date", "y": "total_falls"},
            "tags": ["medical_worker", "admin"]
        },
            
            "System Administrator: Equipment connection status monitoring": {
            "collection": "device",
            "aggregate": [
                {
                    "$sort": {
                        "device_id": 1,
                        "timestamp": -1
                    }
                },
                {
                    "$group": {
                        "_id": "$device_id",
                        "latest_status": {"$first": "$connection_status"},
                        "latest_battery": {"$first": "$battery_level"},
                        "last_update": {"$first": "$timestamp"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "device_id": "$_id",
                        "status": "$latest_status",
                        "battery_level": "$latest_battery",
                        "last_seen": "$last_update"
                    }
                },
                {
                    "$sort": {
                        "battery_level": 1  # 按电量排序，低电量的在前
                    }
                }
            ],
            "chart": {"type": "table"},
            "tags": ["admin"]
        },
            
            
            
            "System Administrator: Count the number of devices with abnormal connection status": {
                "collection": "device",
                "aggregate": [
                    {
                        "$match": {
                            "connection_status": {"$ne": "online"}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$connection_status",  # 按状态分组
                            "count": {"$sum": 1}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "status": "$_id",
                            "device_count": "$count"
                        }
                    },
                    {
                        "$sort": {
                            "device_count": -1
                        }
                    }
                ],
                "chart": {"type": "bar", "x": "status", "y": "device_count"},
                "tags": ["admin"]
            },


            
            "System Administrator: Get the latest device status, battery level, and GPS location for each device ": {
                "collection": "device",
                "aggregate": [
                    {
                        "$sort": {
                            "device_id": 1,
                            "ts": -1
                        }
                    },
                    {
                        "$group": {
                            "_id": "$device_id",
                            "latest_status": {"$first": "$device_status"},
                            "latest_battery": {"$first": "$battery_level"},
                            "latest_gps": {"$first": "$gps_location"},
                            "last_update": {"$first": "$ts"}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "device_id": "$_id",
                            "status": "$latest_status",
                            "battery": "$latest_battery",
                            "gps_location": "$latest_gps",
                            "last_seen": "$last_update"
                        }
                    },
                    {
                        "$sort": {
                            "device_id": 1
                        }
                    }
                ],
                "chart": {"type": "scatter","x": "device_id","y": "battery","color": "status","size": "battery"},
                "tags": ["admin"]
            }
        

}}}

        
            
            
            

           
        
    

# The following block of code will create a simple Streamlit dashboard page
st.set_page_config(page_title="Smart Health Monitoring and Alert System", layout="wide")
st.title("Smart Health Monitoring and Alert System | Mini Dashboard (Postgres + MongoDB)")

def metric_row(metrics: dict):
    cols = st.columns(len(metrics))
    for (k, v), c in zip(metrics.items(), cols):
        c.metric(k, v)

@st.cache_resource
def get_pg_engine(uri: str):
    return create_engine(uri, pool_pre_ping=True, future=True)


def run_pg_query(_engine, sql: str, params: dict | None = None):
    try:
        if not isinstance(sql, str):
            sql = str(sql)
        
        with _engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
            
    except Exception as e:
        st.error(f"Postgres error: {e}")
        return pd.DataFrame()


@st.cache_resource
def get_mongo_client(uri: str):
    return MongoClient(uri)

def mongo_overview(client: MongoClient, db_name: str):
    info = client.server_info()
    db = client[db_name]
    colls = db.list_collection_names()
    stats = db.command("dbstats")
    total_docs = sum(db[c].estimated_document_count() for c in colls) if colls else 0
    return {
        "DB": db_name,
        "Collections": f"{len(colls):,}",
        "Total docs (est.)": f"{total_docs:,}",
        "Storage": f"{round(stats.get('storageSize',0)/1024/1024,1)} MB",
        "Version": info.get("version", "unknown")
    }

@st.cache_data(ttl=60)
def run_mongo_aggregate(_client, db_name: str, coll: str, stages: list):
    db = _client[db_name]
    docs = list(db[coll].aggregate(stages, allowDiskUse=True))
    return pd.json_normalize(docs) if docs else pd.DataFrame()

def render_chart(df: pd.DataFrame, spec: dict):
    if df.empty:
        st.info("No rows.")
        return
    ctype = spec.get("type", "table")
    # light datetime parsing for x axes
    for c in df.columns:
        if df[c].dtype == "object":
            try:
                df[c] = pd.to_datetime(df[c])
            except Exception:
                pass

    if ctype == "table":
        st.dataframe(df, use_container_width=True)
    elif ctype == "line":
        st.plotly_chart(px.line(df, x=spec["x"], y=spec["y"], color=spec["color"]), use_container_width=True)
    elif ctype == "bar":
        st.plotly_chart(px.bar(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "pie":
        st.plotly_chart(px.pie(df, names=spec["names"], values=spec["values"]), use_container_width=True)
    elif ctype == "heatmap":
        pivot = pd.pivot_table(df, index=spec["rows"], columns=spec["cols"], values=spec["values"], aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, aspect="auto", origin="upper",
                                  labels=dict(x=spec["cols"], y=spec["rows"], color=spec["values"])),
                        use_container_width=True)
    elif ctype == "treemap":
        st.plotly_chart(px.treemap(df, path=spec["path"], values=spec["values"]), use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)

# The following block of code is for the dashboard sidebar, where you can pick your users, provide parameters, etc.
with st.sidebar:
    st.header("Connections")
    # These fields are pre-filled from .env file
    pg_uri = st.text_input("Postgres URI", CONFIG["postgres"]["uri"])     
    mongo_uri = st.text_input("Mongo URI", CONFIG["mongo"]["uri"])        
    mongo_db = st.text_input("Mongo DB name", CONFIG["mongo"]["db_name"]) 
    st.divider()
    auto_run = st.checkbox("Auto-run on selection change", value=False, key="auto_run_global")

    st.header("Role & Parameters")
    # CHANGE: Change the different roles, the specific attributes, parameters used, etc., to match your own Information System
    
    role = st.selectbox("User role", ["elderly", "medical_worker", "emergency_contact", "system_administrator", "all"], index=4)
    elderly_id = st.number_input("elderly_id", min_value=1, value=1, step=1)
    medical_worker_id = st.number_input("medical_worker_id", min_value=1, value=1, step=1)
    emergency_contact_id = st.number_input("emergency_contact_id", min_value=1, value=1, step=1)
    elderly_name = st.text_input("elderly_name", value="Amy")
    system_administrator_name= st.text_input("system_administrator_name", value="Admin")
    age_threshold = st.number_input("age_threshold", min_value=0, value=75, step=1)
    days = st.slider("last N days", 1, 90, 7)
    med_low_threshold = st.number_input("med_low_threshold", min_value=0, value=5, step=1)
    reorder_threshold = st.number_input("reorder_threshold", min_value=0, value=10, step=1)

    PARAMS_CTX = {
    "elderly_id": int(elderly_id),
    "medical_worker_id": int(medical_worker_id),
    "emergency_contact_id": int(emergency_contact_id),
    "system_administrator_name": system_administrator_name,
    "elderly_name": elderly_name,
    "age_threshold": int(age_threshold),
    "days": int(days),
    "med_low_threshold": int(med_low_threshold),
    "reorder_threshold": int(reorder_threshold),
    }




#Postgres part of the dashboard
st.subheader("Postgres")
try:
    
    eng = get_pg_engine(pg_uri)
    
    with st.expander("Run Postgres query", expanded=True):
        # The following will filter queries by role
        def filter_queries_by_role(qdict: dict, role: str) -> dict:
            def ok(tags):
                t = [s.lower() for s in (tags or ["all"])]
                return "all" in t or role.lower() in t
            return {name: q for name, q in qdict.items() if ok(q.get("tags"))}

        pg_all = CONFIG["postgres"]["queries"]
        pg_q = filter_queries_by_role(pg_all, role)

        names = list(pg_q.keys()) or ["(no queries for this role)"]
        sel = st.selectbox("Choose a saved query", names, key="pg_sel")

        if sel in pg_q:
            q = pg_q[sel]
            sql = qualify(q["sql"])   
            st.code(sql, language="sql")
            sql = qualify(q["sql"])


            run  = auto_run or st.button("▶ Run Postgres", key="pg_run")
            if run:
                wanted = q.get("params", [])
                params = {k: PARAMS_CTX[k] for k in wanted}
                df = run_pg_query(eng, sql, params=params)
                render_chart(df, q["chart"])
        else:
            st.info("No Postgres queries tagged for this role.")
except Exception as e:
    st.error(f"Postgres error: {e}")


# Mongo panel
if CONFIG["mongo"]["enabled"]:
    st.subheader("🍃 MongoDB")
    try:
        mongo_client = get_mongo_client(mongo_uri)   
        metric_row(mongo_overview(mongo_client, mongo_db))

        with st.expander("Run Mongo aggregation", expanded=True):
            mongo_query_names = list(CONFIG["mongo"]["queries"].keys())
            selm = st.selectbox("Choose a saved aggregation", mongo_query_names, key="mongo_sel")
            q = CONFIG["mongo"]["queries"][selm]
            st.write(f"**Collection:** `{q['collection']}`")
            st.code(str(q["aggregate"]), language="python")
            runm = auto_run or st.button("▶ Run Mongo", key="mongo_run")
            if runm:
                dfm = run_mongo_aggregate(mongo_client, mongo_db, q["collection"], q["aggregate"])
                render_chart(dfm, q["chart"])
    except Exception as e:
        st.error(f"Mongo error: {e}")