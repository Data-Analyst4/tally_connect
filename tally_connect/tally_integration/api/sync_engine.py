def get_advanced_mappings(doc, voucher_type=None):
    """Get advanced mappings from ERPNext Tally Mapping DocType"""
    filters = {
        "doctype": doc.doctype,  # ERPNext DocType name
        "active": "Yes",         # Match CSV "Yes"
        "is_required": ["in", ["Yes", "No"]]  # Both required & optional
    }
    
    # Company filter
    if doc.company:
        filters["company"] = doc.company
    
    # Voucher type filter
    if voucher_type:
        filters["apply_for_voucher_type"] = voucher_type
    
    # ✅ FIXED: Use correct DocType name
    mappings = frappe.get_all("ERPNext Tally Mapping", filters=filters,
        fields=["*"],
        order_by="sequence_order asc"
    )
    
    # Group by category for XML generation
    categorized = {}
    for mapping in mappings:
        category = mapping.category or "General"
        if category not in categorized:
            categorized[category] = []
        categorized[category].append(mapping)
    
    print(f"📋 Loaded {len(mappings)} mappings from ERPNext Tally Mapping")
    return categorized


def apply_advanced_transformation(doc, mapping):
    """Apply transformation based on transformation_type and JSON config"""
    field_value = doc.get(mapping.erpnext_field)
    
    # Apply transformation_type logic
    transformation_type = mapping.transformation_type or "direct"
    
    if transformation_type == "direct":
        return field_value
    
    elif transformation_type == "escape_xml":
        return escape_xml(field_value)
    
    elif transformation_type == "date_format":
        return format_date_for_tally(field_value)
    
    elif transformation_type == "negative":
        return -1 * float(field_value or 0)
    
    elif transformation_type == "json_config" and mapping.transformation_config:
        try:
            config = json.loads(mapping.transformation_config)
            # Handle complex transformations (regex, concat, conditional)
            if config.get("type") == "concat":
                parts = []
                for part in config.get("parts", []):
                    if part.startswith("field:"):
                        parts.append(doc.get(part[6:]))
                    else:
                        parts.append(part)
                return "".join(str(p) for p in parts if p)
        except:
            pass
    
    # Fallback to default value
    return mapping.default_value or field_value

@frappe.whitelist()
def sync_sales_invoice_to_tally(invoice_name, voucher_type="Sales"):
    """Enhanced sync using your advanced mapping structure"""
    try:
        invoice = frappe.get_doc("Sales Invoice", invoice_name)
        
        # Get categorized mappings
        mappings = get_advanced_mappings(invoice, voucher_type)
        print(f"📋 Loaded mappings: {mappings.keys()}")
        
        # Build XML by category sequence
        xml_parts = build_xml_by_category(invoice, mappings)
        
        # Send to Tally
        tally_url = frappe.db.get_single_value("Tally Connect Settings", "tally_url")
        response = frappe.request.post(tally_url, data=xml_parts, 
                                     headers={"Content-Type": "text/xml; charset=utf-8"})
        
        if "<CREATED>1</CREATED>" in response.text:
            frappe.db.set_value("Sales Invoice", invoice.name, {
                "custom_tally_synced": 1,
                "custom_tally_voucher_number": invoice.name,
                "custom_tally_sync_date": frappe.utils.now(),
                "custom_tally_voucher_type": voucher_type
            })
            frappe.db.commit()
            return {"success": True, "voucher_number": invoice.name}
        else:
            return {"success": False, "error": extract_tally_error(response.text)}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

def build_xml_by_category(doc, mappings):
    """Build XML following category sequence order"""
    category_order = ["Header", "Inventory", "Ledger", "Tax", "General"]
    
    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ENVELOPE>',
        ' <HEADER><TALLYREQUEST>Import Data</TALLYREQUEST></HEADER>',
        ' <BODY><IMPORTDATA>',
        '  <REQUESTDESC><REPORTNAME>Vouchers</REPORTNAME>'
    ]
    
    # Add company from Company settings
    company_doc = frappe.get_doc("Company", doc.company)
    xml_parts.append(f'   <STATICVARIABLES><SVCURRENTCOMPANY>{escape_xml(company_doc.custom_tally_company_name)}</SVCURRENTCOMPANY></STATICVARIABLES>')
    
    xml_parts.extend(['  </REQUESTDESC>', '  <REQUESTDATA><TALLYMESSAGE xmlns:UDF="TallyUDF">'])
    
    # Process categories in order
    for category in category_order:
        if category in mappings:
            for mapping in mappings[category]:
                value = apply_advanced_transformation(doc, mapping)
                if value is not None:
                    escaped_value = escape_xml(str(value))
                    xml_parts.append(f'   <{mapping.tally_xml_tag}>{escaped_value}</{mapping.tally_xml_tag}>')
    
    # Add inventory items (special handling)
    if "Inventory" in mappings:
        items_xml = build_items_from_mappings(doc, mappings["Inventory"])
        xml_parts.append(items_xml)
    
    xml_parts.extend(['  </TALLYMESSAGE>', '  </REQUESTDATA>', ' </IMPORTDATA>', '</BODY>', '</ENVELOPE>'])
    
    return "\n".join(xml_parts)

def build_items_from_mappings(doc, item_mappings):
    """Build inventory items using item mappings"""
    items_xml = ""
    for item_row in doc.items:
        # For each item row, apply item mappings
        item_xml = "   <ALLINVENTORYENTRIES.LIST>"
        for mapping in item_mappings:
            if mapping.erpnext_field in ["item_code", "item_name", "qty", "rate", "amount"]:
                # Special handling for child table fields
                value = item_row.get(mapping.erpnext_field) if hasattr(item_row, mapping.erpnext_field) else ""
                item_xml += f"\n    <{mapping.tally_xml_tag}>{escape_xml(str(value))}</{mapping.tally_xml_tag}>"
        item_xml += "\n   </ALLINVENTORYENTRIES.LIST>"
        items_xml += item_xml
    return items_xml
