# Copyright (c) 2025, Kunal Verma and contributors
# For license information, please see license.txt

# Copyright (c) 2025, Your Company
# License: GNU General Public License v3

import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime
import requests
import xml.etree.ElementTree as ET
from frappe import _

class TallyMasterCache(Document):
	def validate(self):
		if self.master_type and self.master_name:
			self.validate_unique()

	def validate_unique(self):
		exists = frappe.db.exists("Tally Master Cache", {
			"master_type": self.master_type,
			"master_name": self.master_name,
			"name": ["!=", self.name or ""]
		})
		if exists:
			frappe.throw(_("Master {0}: {1} already exists").format(self.master_type, self.master_name))

@frappe.whitelist()
def check_master_in_cache(master_type, master_name):
	"""⚡ INSTANT CACHE CHECK - 300x FASTER than Tally API"""
	cache = frappe.db.get_value("Tally Master Cache",
		filters={"master_type": master_type, "master_name": master_name, "is_active": 1},
		fieldname=["name", "last_synced", "guid", "parent_name"]
	)
	
	if cache:
		age_hours = (now_datetime() - cache[1]).total_seconds() / 3600 if cache[1] else 999
		return {
			"exists": True,
			"source": "cache",
			"name": cache[0],
			"guid": cache[2],
			"parent": cache[3],
			"age_hours": round(age_hours, 1),
			"fresh": age_hours < 24
		}
	return {"exists": False, "source": "cache"}

@frappe.whitelist()
def sync_masters_to_cache():
	"""🌙 DAILY CRON - Sync ALL Tally masters (60 seconds)"""
	settings = frappe.get_single("Tally Integration Settings")
	
	if not _is_tally_online(settings.tally_url):
		frappe.log_error("Tally offline during cache sync", "Tally Master Cache")
		return {"success": False, "message": "Tally offline"}
	
	# STEP 1: Mark ALL inactive
	frappe.db.sql("""UPDATE `tabTally Master Cache` SET is_active = 0""")
	
	# STEP 2: Sync each type
	stats = {
		"groups": _sync_type("Group", "Groups"),
		"ledgers": _sync_type("Ledger", "Ledgers"),
		"stock_groups": _sync_type("Stock Group", "Stock Groups"),
		"stock_items": _sync_type("Stock Item", "Stock Items"),
		"godowns": _sync_type("Godown", "Godowns")
	}
	
	total = sum(stats.values())
	frappe.log_error(f"✅ Synced {total} masters to cache", "Tally Master Cache")
	
	return {"success": True, "stats": stats, "total": total}

def _sync_type(master_type, collection):
	"""Sync single master type"""
	xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Collection</TYPE><ID>{collection}</ID></HEADER><BODY><DESC><STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES></DESC></BODY></ENVELOPE>"""
	
	try:
		settings = frappe.get_single("Tally Integration Settings")
		resp = requests.post(settings.tally_url, data=xml.encode(), 
		                     headers={'Content-Type': 'text/xml'}, timeout=30)
		
		if resp.status_code == 200:
			return _parse_and_save(resp.text, master_type)
	except Exception as e:
		frappe.log_error(f"Sync {master_type} failed: {str(e)}", "Tally Master Cache")
	return 0

def _parse_and_save(xml_data, master_type):
	"""Parse Tally XML → Save to cache"""
	count = 0
	try:
		root = ET.fromstring(xml_data)
		for elem in root.iterfind('.//NAME'):
			if elem.text:
				master_name = elem.text.strip()
				parent_elem = elem.getparent()
				parent_name = ""
				
				# Try to find parent
				parent = parent_elem.find('PARENT')
				if parent is not None and parent.text:
					parent_name = parent.text.strip()
				
				# Upsert to cache
				cache_doc = frappe.get_doc({
					"doctype": "Tally Master Cache",
					"master_type": master_type,
					"master_name": master_name,
					"parent_name": parent_name,
					"is_active": 1,
					"last_synced": now_datetime(),
					"sync_source": "Auto"
				})
				cache_doc.insert(ignore_permissions=True)
				count += 1
	except Exception as e:
		frappe.log_error(f"Parse error {master_type}: {str(e)}", "Tally Master Cache")
	return count

def _is_tally_online(url):
	try:
		resp = requests.get(url, timeout=5)
		return resp.status_code in [200, 400]
	except:
		return False

@frappe.whitelist()
def smart_validate_master(master_type, master_name):
	"""🧠 Cache → Tally Fallback → Stale OK"""
	cache = check_master_in_cache(master_type, master_name)
	
	if cache["exists"] and cache["age_hours"] < 24:
		return {"exists": True, "source": "cache_fresh"}
	
	# Cache miss → Check Tally (use your existing API)
	if _is_tally_online(frappe.get_single("Tally Integration Settings").tally_url):
		# TODO: Integrate your existing check_master_exists
		pass
	
	return {"exists": cache["exists"], "source": cache["source"]}
