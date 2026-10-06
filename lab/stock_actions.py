"""
Action tools for the Fashion Inventory Alert Processor.

They let the agent act on its decision instead of only recommending one:

    create_restock_order    -> a logistics order on the fashion.logistics.orders topic
    open_servicenow_ticket  -> a ServiceNow incident on the servicenow.incidents topic

Both write to Kafka through Confluent Cloud's REST API, so they need nothing beyond the
Python standard library and run inside watsonx Orchestrate. Credentials come from the
key-value connection "confluent_rest" (lab Section 4.7).

The ServiceNow tool is a stand-in: it writes the record that the ServiceNow Table API
(POST /api/now/table/incident) would create. To use a real instance, replace the
_publish call in open_servicenow_ticket with that request.
"""

import base64
import json
import random
import urllib.request
import uuid
from datetime import datetime, timezone

from ibm_watsonx_orchestrate.agent_builder.connections import ConnectionType, ExpectedCredentials
from ibm_watsonx_orchestrate.agent_builder.tools import ToolPermission, tool
from ibm_watsonx_orchestrate.run import connections

APP_ID = "confluent_rest"
ORDERS_TOPIC = "fashion.logistics.orders"
INCIDENTS_TOPIC = "servicenow.incidents"
AGENT_NAME = "Fashion_Inventory_Alert_Processor"

# Guardrail in code, not in the prompt: the agent cannot talk its way past it.
# Orders above this value, or with an unknown cost, wait for a person to approve them.
APPROVAL_LIMIT_USD = 20000
PRIORITIES = {"RUSH_EXPEDITED", "RUSH", "STANDARD_PRIORITY", "STANDARD"}
SERVICENOW_LEVELS = {"CRITICAL": "1", "HIGH": "2", "MEDIUM": "3", "LOW": "3"}

CREDENTIALS = [ExpectedCredentials(app_id=APP_ID, type=ConnectionType.KEY_VALUE)]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _publish(topic: str, key: str, record: dict) -> dict:
    """Produces one JSON record with the Confluent Cloud REST API (v3)."""
    creds = connections.key_value(APP_ID)
    url = (f"{creds['rest_endpoint'].rstrip('/')}/kafka/v3/clusters/{creds['cluster_id']}"
           f"/topics/{topic}/records")
    auth = base64.b64encode(f"{creds['api_key']}:{creds['api_secret']}".encode()).decode()
    body = json.dumps({"key": {"type": "STRING", "data": key},
                       "value": {"type": "JSON", "data": record}}).encode()
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Basic {auth}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response)
    if result.get("error_code", 200) >= 400:
        raise RuntimeError(result.get("message", "Confluent REST API error"))
    return result


@tool(permission=ToolPermission.READ_WRITE, expected_credentials=CREDENTIALS)
def create_restock_order(alert_id: str, sku: str, store_id: str, quantity: int, priority: str,
                         current_stock: int, reason: str, unit_price: float = 0.0) -> dict:
    """
    Places a restock order with the logistics team for one product at one store.

    Call this at most once per alert, only after deciding a reorder is needed.

    Args:
        alert_id (str): The alertId of the velocity alert being handled.
        sku (str): The SKU to restock, exactly as in the alert.
        store_id (str): The store to deliver to, exactly as in the alert.
        quantity (int): Units to order. Must be greater than zero.
        priority (str): One of RUSH_EXPEDITED, RUSH, STANDARD_PRIORITY, STANDARD.
        current_stock (int): Units on hand when the order is placed, from the alert.
        reason (str): One sentence explaining why the order is needed.
        unit_price (float): Unit cost from the product history, or 0 if unknown.

    Returns:
        dict: orderId, status (SUBMITTED or PENDING_APPROVAL), estimatedCost and
        approvalRequired, or an error message.
    """
    try:
        if quantity <= 0:
            return {"error": "quantity must be greater than zero"}
        if priority not in PRIORITIES:
            return {"error": f"priority must be one of {sorted(PRIORITIES)}"}

        cost = round(quantity * unit_price, 2) if unit_price > 0 else None
        approval = cost is None or cost > APPROVAL_LIMIT_USD
        order = {
            "orderId": f"PO-{datetime.now(timezone.utc):%Y%m%d}-{uuid.uuid4().hex[:6].upper()}",
            "alertId": alert_id, "sku": sku, "storeId": store_id,
            "quantity": quantity, "priority": priority, "currentStock": current_stock,
            "unitPrice": unit_price or None, "estimatedCost": cost,
            "status": "PENDING_APPROVAL" if approval else "SUBMITTED",
            "approvalReason": ("unit price unknown" if cost is None else
                               f"over the ${APPROVAL_LIMIT_USD:,} limit") if approval else None,
            "reason": reason, "createdAt": _now(), "createdBy": AGENT_NAME,
        }
        _publish(ORDERS_TOPIC, sku, order)
        return {"orderId": order["orderId"], "status": order["status"],
                "estimatedCost": cost, "approvalRequired": approval,
                "message": (f"Order waiting for approval: {order['approvalReason']}" if approval
                            else f"Order submitted to logistics: {quantity} units, {priority}")}
    except Exception as exc:
        return {"error": f"Could not place the order: {exc}"}


@tool(permission=ToolPermission.READ_WRITE, expected_credentials=CREDENTIALS)
def open_servicenow_ticket(alert_id: str, sku: str, store_id: str, urgency_level: str,
                           short_description: str, description: str, current_stock: int) -> dict:
    """
    Opens a ServiceNow incident so the store operations team follows up on an inventory alert.

    Call this at most once per alert, only for CRITICAL or HIGH urgency.

    Args:
        alert_id (str): The alertId of the velocity alert being handled.
        sku (str): The affected SKU, exactly as in the alert.
        store_id (str): The affected store, exactly as in the alert.
        urgency_level (str): The decided urgency: CRITICAL, HIGH, MEDIUM or LOW.
        short_description (str): A one-line summary, under 100 characters.
        description (str): What is happening, what was ordered, and what the team should do.
        current_stock (int): Units on hand, from the alert.

    Returns:
        dict: The incident number, sys_id, state and priority, or an error message.
    """
    try:
        level = SERVICENOW_LEVELS.get(urgency_level.upper())
        if not level:
            return {"error": "urgency_level must be CRITICAL, HIGH, MEDIUM or LOW"}

        incident = {
            "number": f"INC{random.randint(10000, 99999):07d}",
            "sys_id": uuid.uuid4().hex,
            "short_description": short_description[:160],
            "description": description,
            "urgency": level, "impact": level, "priority": level,
            "state": "1", "category": "inventory", "assignment_group": "Store Operations",
            "caller_id": AGENT_NAME, "opened_at": _now(),
            "u_alert_id": alert_id, "u_sku": sku, "u_store_id": store_id,
            "u_current_stock": current_stock, "u_urgency_level": urgency_level.upper(),
        }
        _publish(INCIDENTS_TOPIC, sku, incident)
        return {"number": incident["number"], "sys_id": incident["sys_id"], "state": "New",
                "priority": level, "message": f"Incident {incident['number']} opened for Store Operations"}
    except Exception as exc:
        return {"error": f"Could not open the ticket: {exc}"}
