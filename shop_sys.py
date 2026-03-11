# --- shop_sys.py ---

import tkinter as tk
from tkinter import ttk
import qrcode
import socket
import asyncio
import websockets
import json
import threading
from pymongo import MongoClient
from PIL import Image, ImageTk
import io
from typing import Callable
from datetime import datetime
from collections import defaultdict
import pandas as pd
from tkinter import filedialog
from tkcalendar import DateEntry
from calendar import monthrange

# --- MongoDB Setup ---
client = MongoClient("mongodb://localhost:27017/")
db = client["shop_management"]
sales_collection = db["sales"]
purchases_collection = db["purchases"]

# --- Helper Functions ---
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

def generate_qr_code(data):
    qr = qrcode.QRCode(box_size=6, border=2)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    return img

def upsert_item(collection, items):
    for item in items:
        query = {"id": item["id"]}
        update_data = item.copy()
        update_data['last_update'] = datetime.now()
        # Remove _id if present
        if '_id' in update_data:
            del update_data['_id']
        if collection.find_one(query):
            collection.update_one(query, {'$set': update_data})
        else:
            collection.insert_one(update_data)

# Upsert for stocktake records
def upsert_stocktake(collection, items):
    for item in items:
        # Use item, date as unique key
        query = {"item": item.get("item"), "date": item.get("date")}
        update_data = item.copy()
        update_data['last_update'] = datetime.now()
        # Remove _id if present
        if '_id' in update_data:
            del update_data['_id']
        if collection.find_one(query):
            collection.update_one(query, {'$set': update_data})
        else:
            collection.insert_one(update_data)

            
def get_inventory():
    inventory = {}
    for p in purchases_collection.find():
        inventory[p["item"]] = inventory.get(p["item"], 0) + p["quantity"]
    for s in sales_collection.find():
        inventory[s["productId"]] = inventory.get(s["productId"], 0) - s["quantitySold"]
    return inventory

def refresh_data(tree, data):
    tree.delete(*tree.get_children())
    for d in data:
        tree.insert("", "end", values=list(d.values()))

def get_monthly_avg_prices():
        current_month = datetime.now().month
        current_year = datetime.now().year
        buy_totals = defaultdict(lambda: {'total': 0, 'qty': 0})
        sell_totals = defaultdict(lambda: {'total': 0, 'qty': 0})
        # Purchases
        for p in purchases_collection.find():
            date = p.get('date', p.get('Date'))
            try:
                dt = datetime.fromisoformat(date)
            except Exception:
                continue
            if dt.month == current_month and dt.year == current_year:
                item = p.get('item', p.get('Item'))
                qty = p.get('quantity', p.get('Quantity', 0))
                price = p.get('unitPrice', p.get('Price', 0))
                try:
                    qty = int(qty)
                    price = float(price)
                except Exception:
                    continue
                buy_totals[item]['total'] += price * qty
                buy_totals[item]['qty'] += qty
        # Sales
        for s in sales_collection.find():
            date = s.get('date', s.get('Date'))
            try:
                dt = datetime.fromisoformat(date)
            except Exception:
                continue
            if dt.month == current_month and dt.year == current_year:
                item = s.get('productId', s.get('Product'))
                qty = s.get('quantitySold', s.get('Quantity', 0))
                price = s.get('sellingPrice', s.get('Price', 0))
                try:
                    qty = int(qty)
                    price = float(price)
                except Exception:
                    continue
                sell_totals[item]['total'] += price * qty
                sell_totals[item]['qty'] += qty
        avg_buy = {k: (v['total']/v['qty'] if v['qty'] else 0) for k, v in buy_totals.items()}
        avg_sell = {k: (v['total']/v['qty'] if v['qty'] else 0) for k, v in sell_totals.items()}
        return avg_buy, avg_sell

# --- WebSocket Server ---
on_connect_callback = None
connected_clients = set()

def set_on_connect_callback(cb: Callable[[str], None]):
    global on_connect_callback
    on_connect_callback = cb

async def broadcast_sync():
    data = {
        "type": "sync",
        "payload": {
            "sales": list(sales_collection.find()),
            "purchases": list(purchases_collection.find()),
            "stocktakes": list(db["stocktake"].find())
        }
    }
    for ws in list(connected_clients):
        try:
            await ws.send(json.dumps(data, default=str))
        except Exception:
            connected_clients.discard(ws)

async def ws_handler(websocket, path=None):
    if on_connect_callback:
        peer = websocket.remote_address[0] if hasattr(websocket, 'remote_address') else 'Unknown'
        threading.Thread(target=lambda: on_connect_callback(f"Device connected: {peer}"), daemon=True).start()
    connected_clients.add(websocket)
    # Send full sync on connect
    await websocket.send(json.dumps({
        "type": "sync",
        "payload": {
            "sales": list(sales_collection.find()),
            "purchases": list(purchases_collection.find()),
            "stocktakes": list(db["stocktake"].find())
        }
    }, default=str))
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get("type") == "sync":
                    payload = data.get("payload", {})
                    upsert_item(sales_collection, payload.get("sales", []))
                    upsert_item(purchases_collection, payload.get("purchases", []))
                    upsert_stocktake(db["stocktake"], payload.get("stocktakes", []))
                    await websocket.send(json.dumps({"status": "success", "message": "Data synced."}))
                    await broadcast_sync()
                else:
                    await websocket.send(json.dumps({"status": "error", "message": "Unknown type."}))
            except Exception as e:
                await websocket.send(json.dumps({"status": "error", "message": str(e)}))
    finally:
        connected_clients.discard(websocket)

async def ws_main(on_connect=None):
    global on_connect_callback
    on_connect_callback = on_connect
    server = await websockets.serve(ws_handler, get_local_ip(), 8765)
    pass  # Removed console print
    try:
        await server.wait_closed()
    except KeyboardInterrupt:
        print("WebSocket server stopped.")

def start_ws_server(on_connect=None):
    asyncio.run(ws_main(on_connect=on_connect))

# --- Broadcast on local changes ---
def notify_ws_clients():
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(broadcast_sync())
        else:
            loop.run_until_complete(broadcast_sync())
    except RuntimeError:
        asyncio.run(broadcast_sync())

# --- GUI ---
ws_connect_callback = None

def set_ws_connect_callback(cb):
    global ws_connect_callback
    ws_connect_callback = cb

def build_gui():
    root = tk.Tk()
    root.title("Godoliyas App")
    root.geometry("1270x600+35+40")  # Set window size (width x height)
    try:
        root.iconbitmap('icon.ico')
    except Exception:
        # Ignore missing icon on systems where .ico is unavailable
        pass

    tab_control = ttk.Notebook(root)
    tabs = {}
    for name in ["Dashboard", "Sales", "Purchases", "Inventory", "Stocktaking", "Report", "WebSocket QR"]:
        tab = ttk.Frame(tab_control)
        tab_control.add(tab, text=name)
        tabs[name] = tab
    tab_control.pack(expand=1, fill="both")

    # Malibu color palette
    malibu = {
        '50': "#fefefe", '100': '#dcedfd', '200': '#c1e0fc', '300': "#fefefe",
        '400': '#65b2f5', '500': '#4192f0', '600': '#2c75e4', '700': '#235fd2',
        '800': '#234eaa', '900': '#224486', '950': '#192b52',
    }

    # Modern theme using Malibu palette
    style = ttk.Style()
    style.theme_use('clam')
    style.configure('TFrame', background=malibu['50'])
    style.configure('TLabel', background=malibu['50'], font=('Segoe UI', 10), foreground=malibu['900'])
    style.configure('TButton', font=('Segoe UI', 10, 'bold'), padding=6, background=malibu['400'], foreground=malibu['950'])
    style.map('TButton', background=[('active', malibu['300'])], foreground=[('active', malibu['950'])])
    style.configure('Treeview', font=('Segoe UI', 10), rowheight=28, background=malibu['100'], fieldbackground=malibu['100'], foreground=malibu['950'])
    style.configure('Treeview.Heading', font=('Segoe UI', 11, 'bold'), background=malibu['500'], foreground=malibu['50'])
    style.map('Treeview', background=[('selected', malibu['300'])], foreground=[('selected', malibu['950'])])
    style.configure('OddRow', background=malibu['50'])
    style.configure('EvenRow', background=malibu['100'])

    # --- Ensure tables are populated on startup ---
    # These will only show non-deleted records due to filter logic in refresh functions
    # Call after widgets are created
    def initial_refresh():
        try:
            filter_sales_table()
        except Exception:
            pass
        try:
            filter_purchases_table()
        except Exception:
            pass
        try:
            refresh_inventory()
        except Exception:
            pass
        try:
            refresh_dashboard()
        except Exception:
            pass
        try:
            refresh_stock_table()
        except Exception:
            pass
    root.after(0, initial_refresh)

    # Sales Tab
    sales_pane = ttk.PanedWindow(tabs["Sales"], orient=tk.HORIZONTAL)
    sales_pane.pack(fill="both", expand=True, padx=10, pady=10)

    # Left pane: Entry fields and buttons
    left_frame = ttk.Frame(sales_pane, width=270, relief=tk.RIDGE, borderwidth=5, style='TFrame')
    sales_pane.add(left_frame, weight=1)
    right_frame = ttk.Frame(sales_pane, style='TFrame')
    sales_pane.add(right_frame, weight=20)

    # Title for Sales Form
    ttk.Label(left_frame, text="Sales Entry", font=("Segoe UI", 13, "bold"), background=malibu['200'], foreground=malibu['900']).grid(row=0, column=0, columnspan=1, pady=(10, 15), sticky='ew')

    # --- Helper to get unique item list for dropdowns ---
    def get_item_list():
        items = set()
        for p in purchases_collection.find():
            if p.get("isDelete", False) is False:
                items.add(p.get("item", p.get("Item", "")))
        for s in sales_collection.find():
            if s.get("isDelete", False) is False:
                items.add(s.get("productId", s.get("Product", "")))
        return sorted(i for i in items if i)

    # Entry fields (with dropdown for Product and Date picker)
    entry_labels = ["Product", "Quantity", "Price", "Date"]
    entries = {}
    for i, label in enumerate(entry_labels):
        ttk.Label(left_frame, text=label+":", foreground=malibu['950']).grid(row=i+1, column=0, sticky="e", pady=4, padx=6)
        if label == "Product":
            product_var = tk.StringVar()
            product_combo = ttk.Combobox(left_frame, textvariable=product_var, font=('Segoe UI', 10), values=get_item_list())
            product_combo.grid(row=i+1, column=1, pady=4, padx=6)
            product_combo['state'] = 'normal'  # allow typing/search
            entries[label] = product_combo

            def autocomplete_combobox(event, combo=product_combo, var=product_var):
                value = var.get().lower()
                values = get_item_list()
                filtered = [v for v in values if value in v.lower()]
                combo['values'] = filtered if filtered else values
                # Optionally, open dropdown if matches found
            product_combo.bind('<KeyRelease>', autocomplete_combobox)

        elif label == "Date":
            date_entry = DateEntry(left_frame, font=('Segoe UI', 10), date_pattern='yyyy-mm-dd')
            date_entry.grid(row=i+1, column=1, pady=4, padx=6)
            entries[label] = date_entry
        else:
            entry = ttk.Entry(left_frame, font=('Segoe UI', 10), background=malibu['50'], foreground=malibu['950'])
            entry.grid(row=i+1, column=1, pady=4, padx=6)
            entries[label] = entry

    # --- Helper to update dropdown values ---
    def update_dropdowns():
        item_list = get_item_list()
        if "Product" in entries:
            entries["Product"].configure(values=item_list)
        if "Item" in p_entries:
            p_entries["Item"].configure(values=item_list)

    # Button callbacks for Sales
    def add_sale():
        try:
            qty = float(entries["Quantity"].get())
            price = float(entries["Price"].get())
            product_name = entries["Product"].get().title()
            sale = {
                "productId": product_name,
                "quantitySold": qty,
                "sellingPrice": price,
                "total": qty * price,
                "date": entries["Date"].get_date().isoformat() if hasattr(entries["Date"], 'get_date') else datetime.now().isoformat(),
                "last_update": datetime.now()
            }
            sales_collection.insert_one(sale)
            filter_sales_table()
            notify_ws_clients()
            update_dropdowns()
            tk.messagebox.showinfo("Success", "Sale added successfully.")
            clear_sales_entries()
            refresh_inventory()
            refresh_dashboard()
        except Exception as e:
            tk.messagebox.showerror("Error", f"Failed to add sale: {str(e)}")

    def edit_sale():
        selected = sales_tree.selection()
        if not selected:
            tk.messagebox.showwarning("Select row", "Select a row to edit.")
            return
        item = sales_tree.item(selected[0])
        db_id = item["values"][0]
        from bson import ObjectId
        try:
            qty = float(entries["Quantity"].get())
            price = float(entries["Price"].get())
            product_name = entries["Product"].get().title()
            new_data = {
                "productId": product_name,
                "quantitySold": qty,
                "sellingPrice": price,
                "total": qty * price,
                "date": entries["Date"].get_date().isoformat() if hasattr(entries["Date"], 'get_date') else datetime.now().isoformat(),
                "last_update": datetime.now()
            }
            # Remove _id if present
            if '_id' in new_data:
                del new_data['_id']
            sales_collection.update_one({"_id": ObjectId(db_id)}, {"$set": new_data})
            filter_sales_table()
            notify_ws_clients()
            update_dropdowns()
            tk.messagebox.showinfo("Success", "Sale edited successfully.")
            clear_sales_entries()
            refresh_inventory()
            refresh_dashboard()
        except Exception as e:
            tk.messagebox.showerror("Error", f"Failed to edit sale: {str(e)}")

    def delete_sale():
        selected = sales_tree.selection()
        if not selected:
            tk.messagebox.showwarning("Select row", "Select a row to delete.")
            return
        item = sales_tree.item(selected[0])
        db_id = item["values"][0]
        from bson import ObjectId
        try:
            # Soft delete: set isDelete to True, add if missing
            doc = sales_collection.find_one({"_id": ObjectId(db_id)})
            if doc is not None:
                sales_collection.update_one({"_id": ObjectId(db_id)}, {"$set": {"isDelete": True}})
            filter_sales_table()
            notify_ws_clients()
            update_dropdowns()
            tk.messagebox.showinfo("Success", "Sale deleted successfully.")
            clear_sales_entries()
            refresh_inventory()
            refresh_dashboard()
        except Exception as e:
            tk.messagebox.showerror("Error", f"Failed to delete sale: {str(e)}")

    def clear_sales_entries():
        for entry in entries.values():
            if hasattr(entry, 'delete'):
                entry.delete(0, tk.END)
            elif hasattr(entry, 'set'):
                entry.set("")
            elif hasattr(entry, 'set_date'):
                entry.set_date('')
        # Also clear selection in sales_tree
        sales_tree.selection_remove(sales_tree.selection())
    ttk.Button(left_frame, text="Add", command=add_sale, style='TButton').grid(row=len(entry_labels)+2, column=0, pady=12, padx=4, sticky="ew")
    ttk.Button(left_frame, text="Edit", command=edit_sale, style='TButton').grid(row=len(entry_labels)+2, column=1, pady=12, padx=4, sticky="ew")
    ttk.Button(left_frame, text="Delete", command=delete_sale, style='TButton').grid(row=len(entry_labels)+3, column=0, pady=2, padx=4, sticky="ew")
    ttk.Button(left_frame, text="Clear", command=clear_sales_entries, style='TButton').grid(row=len(entry_labels)+3, column=1, pady=2, padx=4, sticky="ew")

    # Right pane: Treeview with modern look
    # Sales Treeview with vertical scrollbar
    sales_tree_frame = tk.Frame(right_frame)
    sales_tree_frame.pack(fill="both", expand=True, padx=10, pady=10)
    sales_scrollbar = ttk.Scrollbar(sales_tree_frame, orient="vertical")
    sales_tree = ttk.Treeview(sales_tree_frame, columns=("DB id", "Product", "Quantity", "Price", "Total" ,"Date"), show="headings", selectmode="browse", style='Treeview', yscrollcommand=sales_scrollbar.set)
    sales_scrollbar.config(command=sales_tree.yview)
    sales_scrollbar.pack(side="right", fill="y")
    sales_tree.pack(side="left", fill="both", expand=True)
    # Hide DB id column visually but keep it for selection
    sales_tree.heading("DB id", text="DB id")
    sales_tree.column("DB id", width=0, stretch=False, minwidth=0)
    for col in sales_tree["columns"][1:]:
        sales_tree.heading(col, text=col)
        sales_tree.column(col, anchor="center", minwidth=80, width=110)

    # Fill sales entries when a row is selected
    def on_sales_row_select(event):
        selected = sales_tree.selection()
        if not selected:
            return
        values = sales_tree.item(selected[0])['values']
        columns = sales_tree['columns']
        value_map = {col: val for col, val in zip(columns, values)}
        for label in ["Product", "Quantity", "Price", "Date"]:
            entries[label].delete(0, tk.END)
            col_name = label if label in columns else None
            if col_name is None:
                col_name = label.lower().capitalize() if label.lower().capitalize() in columns else None
            val = value_map.get(col_name, "")
            if label in ("Quantity", "Price"):
                entries[label].insert(0, str(val).replace(",", ""))
            else:
                entries[label].insert(0, val)

    # Bind selection event for sales_tree
    sales_tree.bind('<<TreeviewSelect>>', on_sales_row_select)

    # --- SALES SEARCH ---
    sales_search_var = tk.StringVar()
    sales_filter_var = tk.StringVar(value="Date")
    sales_quantity_min = tk.StringVar()
    sales_quantity_max = tk.StringVar()
    sales_price_min = tk.StringVar()
    sales_price_max = tk.StringVar()
    def filter_sales_table(*args):
        query = sales_search_var.get().lower()
        all_data = list(sales_collection.find())
        filtered = [d for d in all_data if d.get("isDelete", False) is False]
        # Search filter
        if query:
            filtered = [d for d in filtered if any(query in str(v).lower() for v in d.values())]
        # Quantity filter
        min_qty = sales_quantity_min.get().strip()
        max_qty = sales_quantity_max.get().strip()
        if min_qty:
            try:
                min_qty_val = float(min_qty)
                filtered = [d for d in filtered if float(d.get("quantitySold", d.get("Quantity", 0))) >= min_qty_val]
            except Exception:
                pass
        if max_qty:
            try:
                max_qty_val = float(max_qty)
                filtered = [d for d in filtered if float(d.get("quantitySold", d.get("Quantity", 0))) <= max_qty_val]
            except Exception:
                pass
        # Price filter
        min_price = sales_price_min.get().strip()
        max_price = sales_price_max.get().strip()
        if min_price:
            try:
                min_price_val = float(min_price)
                filtered = [d for d in filtered if float(d.get("sellingPrice", d.get("Price", 0))) >= min_price_val]
            except Exception:
                pass
        if max_price:
            try:
                max_price_val = float(max_price)
                filtered = [d for d in filtered if float(d.get("sellingPrice", d.get("Price", 0))) <= max_price_val]
            except Exception:
                pass
        # Sort filter
        sort_by = sales_filter_var.get()
        if sort_by == "Date":
            def get_date(d):
                from datetime import datetime
                val = d.get("date", d.get("Date", ""))
                dt = None
                try:
                    dt = datetime.fromisoformat(val)
                except Exception:
                    try:
                        dt = datetime.strptime(val, "%Y-%m-%d")
                    except Exception:
                        return datetime.min
                # Make all datetimes offset-naive (remove tzinfo)
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                return dt
            filtered.sort(key=get_date, reverse=True)
        elif sort_by == "Alphabetical":
            filtered.sort(key=lambda d: str(d.get("productId", d.get("Product", "")).lower()))
        elif sort_by == "Quantity":
            filtered.sort(key=lambda d: float(d.get("quantitySold", d.get("Quantity", 0))), reverse=True)
        elif sort_by == "Price":
            filtered.sort(key=lambda d: float(d.get("sellingPrice", d.get("Price", 0))), reverse=True)
        elif sort_by == "Total":
            filtered.sort(key=lambda d: float(d.get("total", d.get("Total", 0))), reverse=True)
        refresh_data(sales_tree, filtered)
    sales_search_var.trace_add('write', filter_sales_table)
    search_frame = ttk.Frame(right_frame, style='TFrame')
    search_frame.pack(fill='x', padx=10, pady=(0, 5))
    ttk.Label(search_frame, text='Search:', font=('Segoe UI', 10, 'bold')).pack(side='left', padx=(0, 6))
    sales_search_entry = ttk.Entry(search_frame, textvariable=sales_search_var, font=('Segoe UI', 10), width=20)
    sales_search_entry.pack(side='left', fill='x', expand=True)
    ttk.Label(search_frame, text='Sort By:').pack(side='left', padx=(8,4))
    sales_filter_combo = ttk.Combobox(search_frame, textvariable=sales_filter_var, values=["Date", "Alphabetical", "Quantity", "Price", "Total"], width=12, state="readonly")
    sales_filter_combo.pack(side='left', padx=(0,8))
    ttk.Label(search_frame, text='Min Qty:').pack(side='left', padx=(0,4))
    min_qty_entry = ttk.Entry(search_frame, textvariable=sales_quantity_min, width=6)
    min_qty_entry.pack(side='left', padx=(0,4))
    ttk.Label(search_frame, text='Max Qty:').pack(side='left', padx=(0,4))
    max_qty_entry = ttk.Entry(search_frame, textvariable=sales_quantity_max, width=6)
    max_qty_entry.pack(side='left', padx=(0,4))
    ttk.Label(search_frame, text='Min Price:').pack(side='left', padx=(0,4))
    min_price_entry = ttk.Entry(search_frame, textvariable=sales_price_min, width=6)
    min_price_entry.pack(side='left', padx=(0,4))
    ttk.Label(search_frame, text='Max Price:').pack(side='left', padx=(0,4))
    max_price_entry = ttk.Entry(search_frame, textvariable=sales_price_max, width=6)
    max_price_entry.pack(side='left', padx=(0,4))
    # Bind filter events
    sales_filter_var.trace_add('write', lambda *a: filter_sales_table())
    sales_quantity_min.trace_add('write', lambda *a: filter_sales_table())
    sales_quantity_max.trace_add('write', lambda *a: filter_sales_table())
    sales_price_min.trace_add('write', lambda *a: filter_sales_table())
    sales_price_max.trace_add('write', lambda *a: filter_sales_table())

    # Striped rows for Treeview
    def format_date(date_str):
        try:
            return str(datetime.fromisoformat(date_str).date())
        except Exception:
            return date_str.split()[0] if "-" in date_str else date_str

    def format_money(val):
        try:
            return f"{float(val):,.2f}"
        except Exception:
            # Preserve empty or non-numeric values
            if val is None:
                return ""
            return str(val)

    def refresh_sales_tree(tree, data):
        tree.delete(*tree.get_children())
        idx = 0
        for d in data:
            # Only show if isDelete is False or missing
            if d.get("isDelete", False) is False:
                # If isDelete is missing, set it to False in DB
                if "isDelete" not in d:
                    db_id = str(d.get("_id", ""))
                    if db_id:
                        from bson import ObjectId
                        sales_collection.update_one({"_id": ObjectId(db_id)}, {"$set": {"isDelete": False}})
                    d["isDelete"] = False
                tag = 'OddRow' if idx % 2 else 'EvenRow'
                db_id = str(d.get("_id", ""))
                row = [
                    db_id,
                    d.get("productId", d.get("Product", "")),
                    d.get("quantitySold", d.get("Quantity", "")),
                    format_money(d.get("sellingPrice", d.get("Price", ""))),
                    format_money(d.get("total", d.get("Total", ""))),
                    format_date(d.get("date", d.get("Date", "")))
                ]
                tree.insert("", "end", values=row, tags=(tag,))
                idx += 1
    # Patch refresh_data for sales_tree
    global refresh_data
    def refresh_data(tree, data):
        if tree == sales_tree:
            refresh_sales_tree(tree, data)
        elif tree == purchases_tree:
            refresh_purchases_tree(tree, data)
        else:
            tree.delete(*tree.get_children())
            for d in data:
                tree.insert("", "end", values=list(d.values()))

    # Purchases Tab
    purchases_pane = ttk.PanedWindow(tabs["Purchases"], orient=tk.HORIZONTAL)
    purchases_pane.pack(fill="both", expand=True, padx=10, pady=10)

    # Left pane: Entry fields and buttons
    p_left_frame = ttk.Frame(purchases_pane, width=270, relief=tk.RIDGE, borderwidth=5, style='TFrame')
    purchases_pane.add(p_left_frame, weight=1)
    p_right_frame = ttk.Frame(purchases_pane, style='TFrame')
    purchases_pane.add(p_right_frame, weight=3)

    # Title for Purchases Form
    ttk.Label(p_left_frame, text="Purchases Entry", font=("Segoe UI", 13, "bold"), background=malibu['200'], foreground=malibu['900']).grid(row=0, column=0, columnspan=1, pady=(10, 15), sticky='ew')

    # Entry fields for Purchases (with dropdown for Item and Date picker)
    p_entry_labels = ["Item", "Quantity", "Price", "Date"]
    p_entries = {}
    # First, create all labels and non-DateEntry widgets
    for i, label in enumerate(p_entry_labels):
        ttk.Label(p_left_frame, text=label+":", foreground=malibu['950']).grid(row=i+1, column=0, sticky="e", pady=4, padx=6)
        if label == "Item":
            item_var = tk.StringVar()
            item_combo = ttk.Combobox(p_left_frame, textvariable=item_var, font=('Segoe UI', 10), values=get_item_list())
            item_combo.grid(row=i+1, column=1, pady=4, padx=6)
            item_combo['state'] = 'normal'  # allow typing/search
            p_entries[label] = item_combo

            # --- Auto-complete for Combobox ---
            def autocomplete_combobox(event, combo=item_combo, var=item_var):
                value = var.get().lower()
                values = get_item_list()
                filtered = [v for v in values if value in v.lower()]
                combo['values'] = filtered if filtered else values
            item_combo.bind('<KeyRelease>', autocomplete_combobox)
        elif label != "Date":
            entry = ttk.Entry(p_left_frame, font=('Segoe UI', 10), background=malibu['50'], foreground=malibu['950'])
            entry.grid(row=i+1, column=1, pady=4, padx=6)
            p_entries[label] = entry

    # Now create DateEntry after frame is packed
    p_date_entry = DateEntry(p_left_frame, font=('Segoe UI', 10), date_pattern='yyyy-mm-dd')
    p_date_entry.grid(row=p_entry_labels.index("Date")+1, column=1, pady=4, padx=6)
    p_entries["Date"] = p_date_entry

    # Button callbacks for Purchases
    def add_purchase():
        try:
            qty = float(p_entries["Quantity"].get())
            price = float(p_entries["Price"].get())
            item_name = p_entries["Item"].get().title()
            purchase = {
                "item": item_name,
                "quantity": qty,
                "unitPrice": price,
                "total": qty * price,
                "date": p_entries["Date"].get_date().isoformat() if hasattr(p_entries["Date"], 'get_date') else datetime.now().isoformat(),
                "last_update": datetime.now()
            }
            purchases_collection.insert_one(purchase)
            filter_purchases_table()
            notify_ws_clients()
            update_dropdowns()
            tk.messagebox.showinfo("Success", "Purchase added successfully.")
            clear_purchases_entries()
            refresh_inventory()
            refresh_dashboard()
        except Exception as e:
            tk.messagebox.showerror("Error", f"Failed to add purchase: {str(e)}")

    def edit_purchase():
        selected = purchases_tree.selection()
        if not selected:
            tk.messagebox.showwarning("Select row", "Select a row to edit.")
            return
        item = purchases_tree.item(selected[0])
        db_id = item["values"][0]
        from bson import ObjectId
        try:
            qty = float(p_entries["Quantity"].get())
            price = float(p_entries["Price"].get())
            item_name = p_entries["Item"].get().title()
            new_data = {
                "item": item_name,
                "quantity": qty,
                "unitPrice": price,
                "total": qty * price,
                "date": p_entries["Date"].get_date().isoformat() if hasattr(p_entries["Date"], 'get_date') else datetime.now().isoformat(),
                "last_update": datetime.now()
            }
            # Remove _id if present
            if '_id' in new_data:
                del new_data['_id']
            purchases_collection.update_one({"_id": ObjectId(db_id)}, {"$set": new_data})
            filter_purchases_table()
            notify_ws_clients()
            update_dropdowns()
            tk.messagebox.showinfo("Success", "Purchase edited successfully.")
            clear_purchases_entries()
            refresh_inventory()
            refresh_dashboard()
        except Exception as e:
            tk.messagebox.showerror("Error", f"Failed to edit purchase: {str(e)}")

    def delete_purchase():
        selected = purchases_tree.selection()
        if not selected:
            tk.messagebox.showwarning("Select row", "Select a row to delete.")
            return
        item = purchases_tree.item(selected[0])
        db_id = item["values"][0]
        from bson import ObjectId
        try:
            # Soft delete: set isDelete to True, add if missing
            doc = purchases_collection.find_one({"_id": ObjectId(db_id)})
            if doc is not None:
                purchases_collection.update_one({"_id": ObjectId(db_id)}, {"$set": {"isDelete": True}})
            filter_purchases_table()
            notify_ws_clients()
            update_dropdowns()
            tk.messagebox.showinfo("Success", "Purchase deleted successfully.")
            clear_purchases_entries()
            refresh_inventory()
            refresh_dashboard()
        except Exception as e:
            tk.messagebox.showerror("Error", f"Failed to delete purchase: {str(e)}")

    # Buttons for Purchases
    def clear_purchases_entries():
        for entry in p_entries.values():
            if hasattr(entry, 'delete'):
                entry.delete(0, tk.END)
            elif hasattr(entry, 'set'):
                entry.set("")
            elif hasattr(entry, 'set_date'):
                entry.set_date('')
        # Also clear selection in purchases_tree
        purchases_tree.selection_remove(purchases_tree.selection())
    ttk.Button(p_left_frame, text="Add", command=add_purchase, style='TButton').grid(row=len(p_entry_labels)+2, column=0, pady=12, padx=4, sticky="ew")
    ttk.Button(p_left_frame, text="Edit", command=edit_purchase, style='TButton').grid(row=len(p_entry_labels)+2, column=1, pady=12, padx=4, sticky="ew")
    ttk.Button(p_left_frame, text="Delete", command=delete_purchase, style='TButton').grid(row=len(p_entry_labels)+3, column=0, pady=2, padx=4, sticky="ew")
    ttk.Button(p_left_frame, text="Clear", command=clear_purchases_entries, style='TButton').grid(row=len(p_entry_labels)+3, column=1, pady=2, padx=4, sticky="ew")

    # Right pane: Treeview
    # Purchases Treeview with vertical scrollbar
    purchases_tree_frame = tk.Frame(p_right_frame)
    purchases_tree_frame.pack(fill="both", expand=True, padx=8, pady=8)
    purchases_scrollbar = ttk.Scrollbar(purchases_tree_frame, orient="vertical")
    purchases_tree = ttk.Treeview(purchases_tree_frame, columns=("DB id", "Item", "Quantity", "Price", "Total" ,"Date"), show="headings", style='Treeview', yscrollcommand=purchases_scrollbar.set)
    purchases_scrollbar.config(command=purchases_tree.yview)
    purchases_scrollbar.pack(side="right", fill="y")
    purchases_tree.pack(side="left", fill="both", expand=True)
    # Hide DB id column visually but keep it for selection
    purchases_tree.heading("DB id", text="DB id")
    purchases_tree.column("DB id", width=0, stretch=False, minwidth=0)
    for col in purchases_tree["columns"][1:]:
        purchases_tree.heading(col, text=col)
        purchases_tree.column(col, anchor="center", minwidth=70, width=90)

    # Fill purchase entries when a row is selected
    def on_purchases_row_select(event):
        selected = purchases_tree.selection()
        if not selected:
            return
        values = purchases_tree.item(selected[0])['values']
        columns = purchases_tree['columns']
        value_map = {col: val for col, val in zip(columns, values)}
        for label in ["Item", "Quantity", "Price", "Date"]:
            p_entries[label].delete(0, tk.END)
            col_name = label if label in columns else None
            if col_name is None:
                col_name = label.lower().capitalize() if label.lower().capitalize() in columns else None
            val = value_map.get(col_name, "")
            if label in ("Quantity", "Price"):
                p_entries[label].insert(0, str(val).replace(",", ""))
            else:
                p_entries[label].insert(0, val)

    # Bind selection event for purchases_tree
    purchases_tree.bind('<<TreeviewSelect>>', on_purchases_row_select)

    def refresh_purchases_tree(tree, data):
        tree.delete(*tree.get_children())
        idx = 0
        for d in data:
            # Only show if isDelete is False or missing
            if d.get("isDelete", False) is False:
                # If isDelete is missing, set it to False in DB
                if "isDelete" not in d:
                    db_id = str(d.get("_id", ""))
                    if db_id:
                        from bson import ObjectId
                        purchases_collection.update_one({"_id": ObjectId(db_id)}, {"$set": {"isDelete": False}})
                    d["isDelete"] = False
                tag = 'OddRow' if idx % 2 else 'EvenRow'
                db_id = str(d.get("_id", ""))
                row = [
                    db_id,
                    d.get("item", d.get("Item", "")),
                    d.get("quantity", d.get("Quantity", "")),
                    format_money(d.get("unitPrice", d.get("Price", ""))),
                    format_money(d.get("total", d.get("Total", ""))),
                    format_date(d.get("date", d.get("Date", "")))
                ]
                tree.insert("", "end", values=row, tags=(tag,))
                idx += 1

    # --- PURCHASES SEARCH ---
    purchases_search_var = tk.StringVar()
    purchases_filter_var = tk.StringVar(value="Date")
    purchases_quantity_min = tk.StringVar()
    purchases_quantity_max = tk.StringVar()
    purchases_price_min = tk.StringVar()
    purchases_price_max = tk.StringVar()
    def filter_purchases_table(*args):
        query = purchases_search_var.get().lower()
        all_data = list(purchases_collection.find())
        filtered = [d for d in all_data if d.get("isDelete", False) is False]
        # Search filter
        if query:
            filtered = [d for d in filtered if any(query in str(v).lower() for v in d.values())]
        # Quantity filter
        min_qty = purchases_quantity_min.get().strip()
        max_qty = purchases_quantity_max.get().strip()
        if min_qty:
            try:
                min_qty_val = float(min_qty)
                filtered = [d for d in filtered if float(d.get("quantity", d.get("Quantity", 0))) >= min_qty_val]
            except Exception:
                pass
        if max_qty:
            try:
                max_qty_val = float(max_qty)
                filtered = [d for d in filtered if float(d.get("quantity", d.get("Quantity", 0))) <= max_qty_val]
            except Exception:
                pass
        # Price filter
        min_price = purchases_price_min.get().strip()
        max_price = purchases_price_max.get().strip()
        if min_price:
            try:
                min_price_val = float(min_price)
                filtered = [d for d in filtered if float(d.get("unitPrice", d.get("Price", 0))) >= min_price_val]
            except Exception:
                pass
        if max_price:
            try:
                max_price_val = float(max_price)
                filtered = [d for d in filtered if float(d.get("unitPrice", d.get("Price", 0))) <= max_price_val]
            except Exception:
                pass
        # Sort filter
        sort_by = purchases_filter_var.get()
        if sort_by == "Date":
            def get_date(d):
                from datetime import datetime
                val = d.get("date", d.get("Date", ""))
                dt = None
                try:
                    dt = datetime.fromisoformat(val)
                except Exception:
                    try:
                        dt = datetime.strptime(val, "%Y-%m-%d")
                    except Exception:
                        return datetime.min
                # Make all datetimes offset-naive (remove tzinfo)
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                return dt
            filtered.sort(key=get_date, reverse=True)
        elif sort_by == "Alphabetical":
            filtered.sort(key=lambda d: str(d.get("item", d.get("Item", "")).lower()))
        elif sort_by == "Quantity":
            filtered.sort(key=lambda d: float(d.get("quantity", d.get("Quantity", 0))), reverse=True)
        elif sort_by == "Price":
            filtered.sort(key=lambda d: float(d.get("unitPrice", d.get("Price", 0))), reverse=True)
        elif sort_by == "Total":
            filtered.sort(key=lambda d: float(d.get("total", d.get("Total", 0))), reverse=True)
        refresh_data(purchases_tree, filtered)
    purchases_search_var.trace_add('write', filter_purchases_table)
    p_search_frame = ttk.Frame(p_right_frame, style='TFrame')
    p_search_frame.pack(fill='x', padx=8, pady=(0, 5))
    ttk.Label(p_search_frame, text='Search:', font=('Segoe UI', 10, 'bold')).pack(side='left', padx=(0, 6))
    purchases_search_entry = ttk.Entry(p_search_frame, textvariable=purchases_search_var, font=('Segoe UI', 10), width=20)
    purchases_search_entry.pack(side='left', fill='x', expand=True)
    ttk.Label(p_search_frame, text='Sort By:').pack(side='left', padx=(8,4))
    purchases_filter_combo = ttk.Combobox(p_search_frame, textvariable=purchases_filter_var, values=["Date", "Alphabetical", "Quantity", "Price", "Total"], width=12, state="readonly")
    purchases_filter_combo.pack(side='left', padx=(0,8))
    ttk.Label(p_search_frame, text='Min Qty:').pack(side='left', padx=(0,4))
    min_qty_entry = ttk.Entry(p_search_frame, textvariable=purchases_quantity_min, width=6)
    min_qty_entry.pack(side='left', padx=(0,4))
    ttk.Label(p_search_frame, text='Max Qty:').pack(side='left', padx=(0,4))
    max_qty_entry = ttk.Entry(p_search_frame, textvariable=purchases_quantity_max, width=6)
    max_qty_entry.pack(side='left', padx=(0,4))
    ttk.Label(p_search_frame, text='Min Price:').pack(side='left', padx=(0,4))
    min_price_entry = ttk.Entry(p_search_frame, textvariable=purchases_price_min, width=6)
    min_price_entry.pack(side='left', padx=(0,4))
    ttk.Label(p_search_frame, text='Max Price:').pack(side='left', padx=(0,4))
    max_price_entry = ttk.Entry(p_search_frame, textvariable=purchases_price_max, width=6)
    max_price_entry.pack(side='left', padx=(0,4))
    # Bind filter events
    purchases_filter_var.trace_add('write', lambda *a: filter_purchases_table())
    purchases_quantity_min.trace_add('write', lambda *a: filter_purchases_table())
    purchases_quantity_max.trace_add('write', lambda *a: filter_purchases_table())
    purchases_price_min.trace_add('write', lambda *a: filter_purchases_table())
    purchases_price_max.trace_add('write', lambda *a: filter_purchases_table())

    # Inventory Tab
    inventory_frame = ttk.Frame(tabs["Inventory"])
    inventory_frame.pack(fill="both", expand=True)

    # Search and filter controls
    inv_search_var = tk.StringVar()
    inv_filter_var = tk.StringVar(value="Alphabetical")
    inv_quantity_min = tk.StringVar()
    inv_quantity_max = tk.StringVar()

    control_frame = ttk.Frame(inventory_frame)
    control_frame.pack(fill="x", padx=13, pady=6)
    ttk.Label(control_frame, text="Search:").pack(side="left", padx=(0,4))
    inv_search_entry = ttk.Entry(control_frame, textvariable=inv_search_var, width=80)
    inv_search_entry.pack(side="left", padx=(0,8))
    ttk.Label(control_frame, text="Sort By:").pack(side="left", padx=(0,4))
    inv_filter_combo = ttk.Combobox(control_frame, textvariable=inv_filter_var, values=["Alphabetical", "Quantity", "Profit", "Total Buy", "Total Sell"], width=12, state="readonly")
    inv_filter_combo.pack(side="left", padx=(0,8))
    ttk.Label(control_frame, text=": Min Qty").pack(side="right", padx=(0,4))
    min_qty_entry = ttk.Entry(control_frame, textvariable=inv_quantity_min, width=6)
    min_qty_entry.pack(side="right", padx=(0,4))
    ttk.Label(control_frame, text=": Max Qty").pack(side="right", padx=(0,4))
    max_qty_entry = ttk.Entry(control_frame, textvariable=inv_quantity_max, width=6)
    max_qty_entry.pack(side="right", padx=(0,4))

    # Inventory Treeview with vertical scrollbar
    inventory_tree_frame = tk.Frame(inventory_frame)
    inventory_tree_frame.pack(fill="both", expand=True, padx=10, pady=8)
    inventory_scrollbar = ttk.Scrollbar(inventory_tree_frame, orient="vertical")
    inventory_tree = ttk.Treeview(inventory_tree_frame, columns=("Item", "Quantity", "Total Buy", "Total Sell", "Profit"), show="headings", yscrollcommand=inventory_scrollbar.set)
    inventory_scrollbar.config(command=inventory_tree.yview)
    inventory_scrollbar.pack(side="right", fill="y")
    inventory_tree.pack(side="left", fill="both", expand=True)
    for col in inventory_tree["columns"]:
        inventory_tree.heading(col, text=col)
        inventory_tree.column(col, anchor="center", minwidth=80, width=120)

    # Summary shown below inventory (sum of actual profit made from sold items)
    inv_summary_var = tk.StringVar()
    inv_summary_label = ttk.Label(inventory_frame, textvariable=inv_summary_var, font=("Segoe UI", 10))
    inv_summary_label.pack(fill='x', padx=12, pady=(4, 6))

    def get_inventory_data():
        # Build totals and quantities from purchases and sales (ignore soft-deleted records)
        from collections import defaultdict
        inventory = {}
        buy_qty = defaultdict(float)
        buy_totals = defaultdict(float)
        sell_qty = defaultdict(float)
        sell_totals = defaultdict(float)

        # Purchases: accumulate inventory and buy totals/qty
        for p in purchases_collection.find():
            if p.get("isDelete", False) is False:
                item = p.get("item", p.get("Item", ""))
                qty = float(p.get("quantity", p.get("Quantity", 0)))
                price = float(p.get("unitPrice", p.get("Price", 0)))
                # keep inventory as float to support fractional quantities
                inventory[item] = inventory.get(item, 0) + qty
                buy_qty[item] += qty
                buy_totals[item] += price * qty

        # Sales: reduce inventory and accumulate sell totals/qty
        for s in sales_collection.find():
            if s.get("isDelete", False) is False:
                item = s.get("productId", s.get("Product", ""))
                qty = float(s.get("quantitySold", s.get("Quantity", 0)))
                price = float(s.get("sellingPrice", s.get("Price", 0)))
                # keep inventory as float to support fractional quantities
                inventory[item] = inventory.get(item, 0) - qty
                sell_qty[item] += qty
                sell_totals[item] += price * qty

        # Compute per-item profit based on actual sold units.
        # We cost sold units using the average purchase price (total buy / buy_qty) when available.
        data = []
        all_items = set(list(inventory.keys()) + list(buy_totals.keys()) + list(sell_totals.keys()))
        for item in sorted(all_items):
            qty = inventory.get(item, 0)
            total_buy = float(buy_totals.get(item, 0.0))
            total_sell = float(sell_totals.get(item, 0.0))
            total_buy_qty = float(buy_qty.get(item, 0.0))
            total_sell_qty = float(sell_qty.get(item, 0.0))
            avg_buy_price = (total_buy / total_buy_qty) if total_buy_qty else 0.0
            # Cost of units that have been sold (using average cost)
            cost_of_sold = avg_buy_price * total_sell_qty
            # Profit realized from sold items
            profit_from_sold = total_sell - cost_of_sold
            data.append({
                "Item": item,
                "Quantity": qty,
                "Total Buy": total_buy,
                "Total Sell": total_sell,
                "Profit": profit_from_sold
            })
        return data

    def filter_inventory():
        data = get_inventory_data()
        # Search filter
        search = inv_search_var.get().strip().lower()
        if search:
            data = [d for d in data if search in str(d["Item"]).lower()]
        # Quantity filter
        min_qty = inv_quantity_min.get().strip()
        max_qty = inv_quantity_max.get().strip()
        if min_qty:
            try:
                min_qty_val = int(min_qty)
                data = [d for d in data if d["Quantity"] >= min_qty_val]
            except Exception:
                pass
        if max_qty:
            try:
                max_qty_val = int(max_qty)
                data = [d for d in data if d["Quantity"] <= max_qty_val]
            except Exception:
                pass
        # Sort filter
        sort_by = inv_filter_var.get()
        if sort_by == "Alphabetical":
            data.sort(key=lambda d: str(d["Item"]).lower())
        elif sort_by == "Quantity":
            data.sort(key=lambda d: d["Quantity"], reverse=True)
        elif sort_by == "Profit":
            data.sort(key=lambda d: d["Profit"], reverse=True)
        elif sort_by == "Total Buy":
            data.sort(key=lambda d: d["Total Buy"], reverse=True)
        elif sort_by == "Total Sell":
            data.sort(key=lambda d: d["Total Sell"], reverse=True)
        return data

    def refresh_inventory():
        inventory_tree.delete(*inventory_tree.get_children())
        data = filter_inventory()
        total_actual_profit = 0.0
        for d in data:
            inventory_tree.insert("", "end", values=(d["Item"], d["Quantity"], f"{d['Total Buy']:,.2f}", f"{d['Total Sell']:,.2f}", f"{d['Profit']:,.2f}"))
            try:
                total_actual_profit += float(d.get('Profit', 0.0))
            except Exception:
                pass
        # Display a non-negative (positive) summary if desired by user: show max(0, total)
        positive_sum = max(0.0, total_actual_profit)
        inv_summary_var.set(f"Total actual profit (sold items): {positive_sum:,.2f}")

    # Bind search and filter events
    inv_search_var.trace_add('write', lambda *a: refresh_inventory())
    inv_filter_var.trace_add('write', lambda *a: refresh_inventory())
    inv_quantity_min.trace_add('write', lambda *a: refresh_inventory())
    inv_quantity_max.trace_add('write', lambda *a: refresh_inventory())

    # --- Stocktaking Tab ---
    stocktake_collection = db["stocktake"]
    stock_tab = tabs["Stocktaking"]

    stock_frame = ttk.Frame(stock_tab)
    stock_frame.pack(fill="both", expand=True, padx=10, pady=10)

    # --- Chat-style grouped list view for stocktakes ---
    from collections import defaultdict
    def get_grouped_stocktakes():
        grouped = defaultdict(list)
        for d in stocktake_collection.find():
            date = d.get("date", "")
            grouped[date].append(d)
        return grouped

    # --- Refined Stocktake List UI ---
    list_frame = ttk.Frame(stock_frame, style='TFrame', relief=tk.RIDGE, borderwidth=2)
    list_frame.pack(fill="both", expand=True, padx=8, pady=8)
    # Heading
    heading = ttk.Label(list_frame, text="Stocktake History", font=("Segoe UI", 14, "bold"), background="#c1e0fc", foreground="#234486", anchor="w")
    heading.pack(fill="x", padx=0, pady=(0,6))
    # Listbox with scrollbar
    listbox_frame = ttk.Frame(list_frame)
    listbox_frame.pack(fill="both", expand=True)
    stock_listbox = tk.Listbox(listbox_frame, font=("Segoe UI", 11), activestyle='none', bg="#fefefe", fg="#234486", selectbackground="#65b2f5", selectforeground="#192b52", highlightthickness=0, borderwidth=0)
    stock_listbox.pack(side="left", fill="both", expand=True)
    scrollbar = ttk.Scrollbar(listbox_frame, orient="vertical", command=stock_listbox.yview)
    scrollbar.pack(side="right", fill="y")
    stock_listbox.config(yscrollcommand=scrollbar.set)
    # Add summary info below
    summary_var = tk.StringVar()
    summary_label = ttk.Label(list_frame, textvariable=summary_var, font=("Segoe UI", 10), background="#fefefe", foreground="#234486", anchor="w")
    summary_label.pack(fill="x", padx=0, pady=(6,0))

    # --- Bulk Entry, Edit, Delete Controls ---
    # CellEditor: single overlay Entry that can edit any Treeview cell and tracks repositioning
    class CellEditor:
        def __init__(self, tree: ttk.Treeview):
            self.tree = tree
            self.entry = tk.Entry(tree)
            self._iid = None
            self._col = None
            self._save_cb = None
            self._change_cb = None
            # hide initially
            self.entry.place_forget()
            # reposition on various events (scroll/resize/ expose)
            for ev in ('<Configure>', '<Expose>', '<Motion>', '<Button-1>', '<ButtonRelease-1>', '<MouseWheel>'):
                try:
                    tree.bind(ev, self._reposition, add='+')
                except Exception:
                    pass
            self.entry.bind('<Return>', self._on_return)
            self.entry.bind('<FocusOut>', self._on_focus_out)
            self.entry.bind('<KeyRelease>', self._on_key)

        def show(self, iid, col, text='', save_callback: Callable = None, change_callback: Callable = None):
            self._iid, self._col = iid, col
            self._save_cb = save_callback
            self._change_cb = change_callback
            bbox = self.tree.bbox(iid, col)
            if not bbox:
                # Try again shortly if tree hasn't finished layout
                self.tree.after(80, lambda: self.show(iid, col, text, save_callback, change_callback))
                return
            x, y, w, h = bbox
            self.entry.place(x=x, y=y, width=w, height=h)
            self.entry.delete(0, tk.END)
            self.entry.insert(0, str(text))
            # reset visual state
            try:
                self.entry.config(bg='white')
            except Exception:
                pass
            self.entry.focus_set()

        def get(self):
            return self.entry.get()

        def _on_return(self, e=None):
            val = self.get()
            # Validate integer (allow empty)
            valid = False
            try:
                if str(val).strip() == "":
                    valid = True
                else:
                    int(val)
                    valid = True
            except Exception:
                valid = False
            if not valid:
                # mark invalid visually and keep editor open
                try:
                    self.entry.config(bg='#ffcccc')
                except Exception:
                    pass
                return
            # commit
            if self._save_cb:
                try:
                    self._save_cb(self._iid, self._col, val)
                except Exception:
                    pass
            self.entry.place_forget()

        def _on_focus_out(self, e=None):
            # On focus-out, attempt to save (same as Return) so edits aren't lost when user clicks Save
            self._on_return()

        def _on_key(self, e=None):
            val = self.get()
            # live-validate: numeric or empty
            try:
                if str(val).strip() == "":
                    self.entry.config(bg='white')
                else:
                    int(val)
                    self.entry.config(bg='white')
            except Exception:
                try:
                    self.entry.config(bg='#ffcccc')
                except Exception:
                    pass
            if self._change_cb:
                try:
                    self._change_cb(self._iid, self._col, val)
                except Exception:
                    pass

        def _reposition(self, e=None):
            if not self._iid or not self._col:
                return
            bbox = self.tree.bbox(self._iid, self._col)
            if not bbox:
                self.entry.place_forget()
                return
            x, y, w, h = bbox
            self.entry.place(x=x, y=y, width=w, height=h)

    control_btn_frame = ttk.Frame(stock_frame)
    control_btn_frame.pack(fill="x", padx=8, pady=(0,8))
    def open_bulk_entry():
        # Prevent multiple Bulk Entry popups; focus existing if present
        existing = getattr(stock_tab, 'bulk_popup', None)
        if existing and existing.winfo_exists():
            try:
                existing.lift()
                existing.focus_force()
            except Exception:
                pass
            return
        popup = tk.Toplevel(stock_tab)
        # track popup to prevent duplicates
        stock_tab.bulk_popup = popup
        popup.title("Bulk Stocktake Entry")
        popup.geometry("700x490+65+55")
        popup.iconbitmap('icon.ico')
        popup.resizable(True, True)
        popup.attributes('-toolwindow', False)
        popup.attributes('-topmost', False)
        popup.update_idletasks()
        def _on_close_bulk():
            try:
                if getattr(stock_tab, 'bulk_popup', None) is popup:
                    stock_tab.bulk_popup = None
            except Exception:
                pass
            try:
                popup.destroy()
            except Exception:
                pass
        popup.protocol('WM_DELETE_WINDOW', _on_close_bulk)
        popup.bind('<Destroy>', lambda e: setattr(stock_tab, 'bulk_popup', None) if getattr(stock_tab, 'bulk_popup', None) is popup else None)
        
        # Table for entry with vertical scrollbar
        table_frame = ttk.Frame(popup)
        table_frame.pack(fill="both", expand=True, padx=8, pady=8)
        entry_table = ttk.Treeview(table_frame, columns=("Item", "Inventory Qty", "Physical Count", "Difference"), show="headings")
        for col in entry_table["columns"]:
            entry_table.heading(col, text=col)
            entry_table.column(col, anchor="center", minwidth=80, width=120)
        entry_scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=entry_table.yview)
        entry_table.configure(yscrollcommand=entry_scrollbar.set)
        entry_table.pack(side="left", fill="both", expand=True)
        entry_scrollbar.pack(side="right", fill="y")
        # popup.attributes('-toolwindow', False)
        # Ensure default window controls (minimize, maximize, close) are visible
        # Fill with all inventory items and use a single CellEditor for editing the "Physical Count" cell
        inventory = get_inventory_data()
        # Create single overlay editor for this tree
        cell_editor = CellEditor(entry_table)
        for d in inventory:
            entry_table.insert("", "end", values=(d["Item"], d["Quantity"], "", ""))

        # Simple tooltip shown on hover for tree cells
        class _CellTooltip:
            def __init__(self, widget):
                self.widget = widget
                self.tw = None

            def show(self, text, x, y):
                self.hide()
                try:
                    self.tw = tk.Toplevel(self.widget)
                    self.tw.wm_overrideredirect(True)
                    label = tk.Label(self.tw, text=text, background='#ffffe0', relief='solid', borderwidth=1, font=('Segoe UI', 9))
                    label.pack(ipadx=4, ipady=2)
                    self.tw.wm_geometry(f"+{x}+{y}")
                except Exception:
                    self.hide()

            def hide(self):
                if self.tw:
                    try:
                        self.tw.destroy()
                    except Exception:
                        pass
                    self.tw = None

        _tooltip = _CellTooltip(entry_table)

        def _on_table_motion(event):
            iid = entry_table.identify_row(event.y)
            col = entry_table.identify_column(event.x)
            if not iid or not col:
                _tooltip.hide()
                return
            try:
                col_index = int(col.replace('#', '')) - 1
            except Exception:
                _tooltip.hide()
                return
            cols = entry_table['columns']
            if col_index < 0 or col_index >= len(cols):
                _tooltip.hide()
                return
            col_name = cols[col_index]
            vals = list(entry_table.item(iid).get('values', []))
            val = vals[col_index] if col_index < len(vals) else ''
            # Show tooltip near pointer
            try:
                _tooltip.show(f"{col_name}: {val}", event.x_root + 18, event.y_root + 12)
            except Exception:
                _tooltip.hide()

        entry_table.bind('<Motion>', _on_table_motion)
        entry_table.bind('<Leave>', lambda e: _tooltip.hide())

        # live update callback to update Difference column while typing
        def _parse_int_like(s):
            """Parse a value that may be int-like, float-like, or contain commas. Return int fallback 0 on failure."""
            try:
                if s is None:
                    return 0
                s_str = str(s).strip()
                if s_str == "":
                    return 0
                s_str = s_str.replace(',', '')
                try:
                    return int(s_str)
                except Exception:
                    return int(float(s_str))
            except Exception:
                return 0

        def live_update(iid, col, val):
            inv_qty = _parse_int_like(entry_table.item(iid)["values"][1] if len(entry_table.item(iid)["values"]) > 1 else 0)
            phys = _parse_int_like(val)
            try:
                diff = phys - inv_qty
                # label by sign: positive => Purchase, negative => Sales
                label = ''
                try:
                    if diff > 0:
                        label = ' Purchase'
                    elif diff < 0:
                        label = ' Sales'
                except Exception:
                    label = ''
                # update the underlying values list so later reads see the change
                vals = list(entry_table.item(iid).get('values', []))
                while len(vals) < 4:
                    vals.append('')
                vals[3] = f"{diff}{label}"
                entry_table.item(iid, values=vals)
            except Exception:
                vals = list(entry_table.item(iid).get('values', []))
                while len(vals) < 4:
                    vals.append('')
                vals[3] = ''
                entry_table.item(iid, values=vals)

        # save callback to commit edited value into the tree
        def save_phys(iid, col, val):
            # update values list so the Physical Count cell shows the committed value
            vals = list(entry_table.item(iid).get('values', []))
            while len(vals) < 4:
                vals.append('')
            vals[2] = str(val)
            entry_table.item(iid, values=vals)
            # update difference as well
            live_update(iid, col, val)

        # Double-click to edit the Physical Count cell using the CellEditor
        def on_double_click(event):
            item_id = entry_table.identify_row(event.y)
            col = entry_table.identify_column(event.x)
            if not item_id or col != '#3':
                return
            cur = entry_table.item(item_id)["values"][2]
            cell_editor.show(item_id, '#3', text=cur, save_callback=save_phys, change_callback=live_update)
        entry_table.bind('<Double-1>', on_double_click)
        def save_bulk():
            date = datetime.now().isoformat()
            items = []
            for iid in entry_table.get_children():
                vals = entry_table.item(iid)["values"]
                item, inv_qty = vals[0], vals[1]
                # Physical Count is stored in column index 2 of the tree values
                phys_val = vals[2] if len(vals) > 2 else ""
                try:
                    inv_qty = _parse_int_like(inv_qty)
                except Exception:
                    inv_qty = 0
                try:
                    phys_count = _parse_int_like(phys_val) if str(phys_val).strip() != "" else 0
                except Exception:
                    phys_count = 0
                try:
                    difference = phys_count - inv_qty
                except Exception:
                    difference = 0
                items.append({
                    "item": item,
                    "inventory_qty": inv_qty,
                    "physical_count": phys_count,
                    "difference": difference,
                    "date": date
                })
            upsert_stocktake(stocktake_collection, items)
            refresh_stock_list()
            notify_ws_clients()
            try:
                if getattr(stock_tab, 'bulk_popup', None) is popup:
                    stock_tab.bulk_popup = None
            except Exception:
                pass
            try:
                popup.destroy()
            except Exception:
                pass
        # Use a dedicated frame for buttons to avoid mixing pack/grid in the same container
        button_frame = ttk.Frame(popup)
        button_frame.pack(fill='x', padx=8, pady=12)
        ttk.Button(button_frame, text="Save", command=save_bulk, style='TButton').pack(side='right', padx=4)
        ttk.Button(button_frame, text="Cancel", command=_on_close_bulk, style='TButton').pack(side='right', padx=4)
        # (Inline editing handled by CellEditor above)
    def open_edit():
        selection = stock_listbox.curselection()
        if not selection:
            tk.messagebox.showwarning("Select group", "Select a stocktake group to edit.")
            return
        idx = selection[0]
        date = stock_listbox.dates[idx]
        records = get_grouped_stocktakes()[date]
        # Prevent multiple Edit popups; focus existing if present
        existing = getattr(stock_tab, 'edit_popup', None)
        if existing and existing.winfo_exists():
            try:
                existing.lift()
                existing.focus_force()
            except Exception:
                pass
            return
        popup = tk.Toplevel(stock_tab)
        stock_tab.edit_popup = popup
        popup.title(f"Edit Stocktake: {date.replace('T', ' ')}")
        popup.geometry("700x490+65+55")
        popup.iconbitmap('icon.ico')
        popup.resizable(True, True)
        popup.attributes('-toolwindow', False)
        popup.attributes('-topmost', False)
        popup.update_idletasks()
        def _on_close_edit():
            try:
                if getattr(stock_tab, 'edit_popup', None) is popup:
                    stock_tab.edit_popup = None
            except Exception:
                pass
            try:
                popup.destroy()
            except Exception:
                pass
        popup.protocol('WM_DELETE_WINDOW', _on_close_edit)
        popup.bind('<Destroy>', lambda e: setattr(stock_tab, 'edit_popup', None) if getattr(stock_tab, 'edit_popup', None) is popup else None)
        # Table for edit with vertical scrollbar
        table_frame = ttk.Frame(popup)
        table_frame.pack(fill="both", expand=True, padx=8, pady=8)
        table = ttk.Treeview(table_frame, columns=("Item", "Inventory Qty", "Physical Count", "Difference"), show="headings")
        for col in table["columns"]:
            table.heading(col, text=col)
            table.column(col, anchor="center", minwidth=80, width=120)
        table_scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=table.yview)
        table.configure(yscrollcommand=table_scrollbar.set)
        table.pack(side="left", fill="both", expand=True)
        table_scrollbar.pack(side="right", fill="y")
    # Ensure default window controls (minimize, maximize, close) are visible
        popup.attributes('-toolwindow', False)
        # Use a single CellEditor for inline edits instead of per-row placed Entry widgets
        cell_editor = CellEditor(table)
        for d in records:
            table.insert("", "end", values=(d.get("item", ""), d.get("inventory_qty", ""), d.get("physical_count", ""), ""))

        # Simple tooltip for edit table cells
        class _CellTooltip:
            def __init__(self, widget):
                self.widget = widget
                self.tw = None

            def show(self, text, x, y):
                self.hide()
                try:
                    self.tw = tk.Toplevel(self.widget)
                    self.tw.wm_overrideredirect(True)
                    label = tk.Label(self.tw, text=text, background='#ffffe0', relief='solid', borderwidth=1, font=('Segoe UI', 9))
                    label.pack(ipadx=4, ipady=2)
                    self.tw.wm_geometry(f"+{x}+{y}")
                except Exception:
                    self.hide()

            def hide(self):
                if self.tw:
                    try:
                        self.tw.destroy()
                    except Exception:
                        pass
                    self.tw = None

        _tooltip = _CellTooltip(table)

        def _on_table_motion(event):
            iid = table.identify_row(event.y)
            col = table.identify_column(event.x)
            if not iid or not col:
                _tooltip.hide()
                return
            try:
                col_index = int(col.replace('#', '')) - 1
            except Exception:
                _tooltip.hide()
                return
            cols = table['columns']
            if col_index < 0 or col_index >= len(cols):
                _tooltip.hide()
                return
            col_name = cols[col_index]
            vals = list(table.item(iid).get('values', []))
            val = vals[col_index] if col_index < len(vals) else ''
            try:
                _tooltip.show(f"{col_name}: {val}", event.x_root + 18, event.y_root + 12)
            except Exception:
                _tooltip.hide()

        table.bind('<Motion>', _on_table_motion)
        table.bind('<Leave>', lambda e: _tooltip.hide())

        def _parse_int_like(s):
            try:
                if s is None:
                    return 0
                s_str = str(s).strip()
                if s_str == "":
                    return 0
                s_str = s_str.replace(',', '')
                try:
                    return int(s_str)
                except Exception:
                    return int(float(s_str))
            except Exception:
                return 0

        # live update callback to update Difference column while typing
        def live_update(iid, col, val):
            inv_qty = _parse_int_like(table.item(iid)["values"][1] if len(table.item(iid)["values"]) > 1 else 0)
            phys = _parse_int_like(val)
            try:
                diff = phys - inv_qty
                label = ''
                try:
                    if diff > 0:
                        label = ' Purchase'
                    elif diff < 0:
                        label = ' Sales'
                except Exception:
                    label = ''
                vals = list(table.item(iid).get('values', []))
                while len(vals) < 4:
                    vals.append('')
                vals[3] = f"{diff}{label}"
                table.item(iid, values=vals)
            except Exception:
                vals = list(table.item(iid).get('values', []))
                while len(vals) < 4:
                    vals.append('')
                vals[3] = ''
                table.item(iid, values=vals)

        def save_edit():
            items = []
            for iid in table.get_children():
                vals = table.item(iid)["values"]
                item, inv_qty = vals[0], vals[1] if len(vals) > 1 else 0
                phys_val = vals[2] if len(vals) > 2 else ""
                try:
                    inv_qty = _parse_int_like(inv_qty)
                except Exception:
                    continue
                try:
                    phys_count = _parse_int_like(phys_val) if str(phys_val).strip() != "" else 0
                except Exception:
                    phys_count = 0
                items.append({
                    "item": item,
                    "inventory_qty": inv_qty,
                    "physical_count": phys_count,
                    "difference": phys_count - inv_qty,
                    "date": date
                })
            # Remove old records for this date, then upsert
            stocktake_collection.delete_many({"date": date})
            upsert_stocktake(stocktake_collection, items)
            refresh_stock_list()
            notify_ws_clients()
            try:
                if getattr(stock_tab, 'edit_popup', None) is popup:
                    stock_tab.edit_popup = None
            except Exception:
                pass
            try:
                popup.destroy()
            except Exception:
                pass
        ttk.Button(popup, text="Save", command=save_edit, style='TButton').pack(pady=10)
        ttk.Button(popup, text="Cancel", command=_on_close_edit, style='TButton').pack(pady=2)
        # Allow editing physical count in table using CellEditor
        # update Physical Count into the item's values list and refresh Difference
        def table_save_phys(iid, col, val):
            try:
                vals = list(table.item(iid).get('values', []))
                while len(vals) < 4:
                    vals.append('')
                vals[2] = str(val)
                table.item(iid, values=vals)
                live_update(iid, col, val)
            except Exception:
                pass

        def on_double_click(event):
            item_id = table.identify_row(event.y)
            col = table.identify_column(event.x)
            if not item_id or col != '#3':
                return
            cur = table.item(item_id)["values"][2]
            cell_editor.show(item_id, '#3', text=cur, save_callback=table_save_phys, change_callback=live_update)
        table.bind('<Double-1>', on_double_click)
    def delete_group():
        selection = stock_listbox.curselection()
        if not selection:
            tk.messagebox.showwarning("Select group", "Select a stocktake group to delete.")
            return
        idx = selection[0]
        date = stock_listbox.dates[idx]
        if tk.messagebox.askyesno("Delete", f"Delete all stocktake records for {date.replace('T', ' ')}?"):
            stocktake_collection.delete_many({"date": date})
            refresh_stock_list()
            notify_ws_clients()
    ttk.Button(control_btn_frame, text="Bulk Entry", command=open_bulk_entry, style='TButton').pack(side='left', padx=4)
    ttk.Button(control_btn_frame, text="Edit Group", command=open_edit, style='TButton').pack(side='left', padx=4)
    ttk.Button(control_btn_frame, text="Delete Group", command=delete_group, style='TButton').pack(side='left', padx=4)
    def export_selected_group():
        selection = stock_listbox.curselection()
        if not selection:
            tk.messagebox.showwarning("Select group", "Select a stocktake group to export.")
            return
        idx = selection[0]
        date = stock_listbox.dates[idx]
        records = get_grouped_stocktakes()[date]
        import pandas as pd
        from tkinter import filedialog
        try:
            formatted_date = pd.to_datetime(date, errors='coerce').strftime("%B %d, %Y")
        except Exception:
            formatted_date = str(date)
        df = pd.DataFrame([{
            "Item": d.get("item", ""),
            "Inventory Qty": d.get("inventory_qty", ""),
            "Physical Count": d.get("physical_count", ""),
            "Difference": d.get("difference", ""),
            "Date": pd.to_datetime(d.get("date", ""), errors='coerce').strftime('%Y-%m-%d') if d.get("date", "") else ""
        } for d in records])
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=f"Stocktake_{formatted_date}.xlsx",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if not file_path:
            return
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Stocktake", index=False)
        tk.messagebox.showinfo("Exported", f"Stocktake group exported to {file_path}")
    ttk.Button(control_btn_frame, text="Export Group", command=export_selected_group, style='TButton').pack(side='right', padx=4)

    # Refresh list after changes
    def refresh_stock_table():
        refresh_stock_list()

    def refresh_stock_list():
        stock_listbox.delete(0, tk.END)
        grouped = get_grouped_stocktakes()
        # Sort by date descending
        sorted_dates = sorted(grouped.keys(), reverse=True)
        total_items = sum(len(grouped[date]) for date in sorted_dates)
        stock_listbox.delete(0, tk.END)
        for date in sorted_dates:
            items = grouped[date]
            # Format date to show hour and minutes only
            try:
                dt = datetime.fromisoformat(date)
                date_str = dt.strftime('%Y-%b-%d  %H:%M')
            except Exception:
                date_str = date.replace('T', ' ')[:16]  # fallback: cut to minutes
            icon = "🗂 "
            display = f"{icon}{date_str}  •  {len(items)} items"
            stock_listbox.insert(tk.END, display)
        # Attach dates for lookup
        stock_listbox.dates = sorted_dates
        # Update summary info
        summary_var.set(f"Total stocktakes: {len(sorted_dates)}   |   Total items: {total_items}")

    refresh_stock_list()
    # Patch initial_refresh to call refresh_stock_table
    refresh_stock_table = refresh_stock_list

    def refresh_stock_list():
        stock_listbox.delete(0, tk.END)
        grouped = get_grouped_stocktakes()
        # Sort by date descending
        sorted_dates = sorted(grouped.keys(), reverse=True)
        for date in sorted_dates:
            items = grouped[date]
            # Show date and count
            display = f"{date.replace('T', ' ')}  •  {len(items)} items"
            stock_listbox.insert(tk.END, display)
        # Attach dates for lookup
        stock_listbox.dates = sorted_dates

    refresh_stock_list()

    def show_stocktake_details(event):
        selection = stock_listbox.curselection()
        if not selection:
            return
        idx = selection[0]
        date = stock_listbox.dates[idx]
        records = get_grouped_stocktakes()[date]
        # Popup with table for this date
        popup = tk.Toplevel(stock_tab)
        popup.title(f"Stocktake: {date.replace('T', ' ')}")
        popup.geometry("700x490+65+55")
        popup.iconbitmap('icon.ico')
        popup.resizable(True, True)
        popup.attributes('-toolwindow', False)
        popup.attributes('-topmost', False)
        popup.update_idletasks()
        # Table for display with vertical scrollbar
        table_frame = ttk.Frame(popup)
        table_frame.pack(fill="both", expand=True, padx=8, pady=8)
        table = ttk.Treeview(table_frame, columns=("Item", "Inventory Qty", "Physical Count", "Difference"), show="headings")
        for col in table["columns"]:
            table.heading(col, text=col)
            table.column(col, anchor="center", minwidth=80, width=120)
        table_scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=table.yview)
        table.configure(yscrollcommand=table_scrollbar.set)
        table.pack(side="left", fill="both", expand=True)
        table_scrollbar.pack(side="right", fill="y")
        for d in records:
            row = [d.get("item", ""), d.get("inventory_qty", ""), d.get("physical_count", ""), d.get("difference", "")]
            table.insert("", "end", values=row)
    # Ensure default window controls (minimize, maximize, close) are visible
        popup.attributes('-toolwindow', False)

    # Single click: just select (default Listbox behavior)
    # Double click: open details popup
    def on_stock_listbox_double_click(event):
        selection = stock_listbox.curselection()
        if not selection:
            return
        show_stocktake_details(event)
    stock_listbox.bind('<Double-Button-1>', on_stock_listbox_double_click)

# ...existing code...
    # Report Tab
    report_tab = tabs["Report"]
    # Create a scrollable area for the report tab (canvas + inner frame)
    report_container = ttk.Frame(report_tab)
    report_container.pack(fill='both', expand=True)
    report_canvas = tk.Canvas(report_container, highlightthickness=0)
    report_scroll = ttk.Scrollbar(report_container, orient='vertical', command=report_canvas.yview)
    report_canvas.configure(yscrollcommand=report_scroll.set)
    report_scroll.pack(side='right', fill='y')
    report_canvas.pack(side='left', fill='both', expand=True)
    report_frame = ttk.Frame(report_canvas, padding=20)
    report_window = report_canvas.create_window((0,0), window=report_frame, anchor='nw')

    def _on_report_config(event):
        # update scrollregion to encompass inner frame
        try:
            report_canvas.configure(scrollregion=report_canvas.bbox('all'))
        except Exception:
            pass
    report_frame.bind('<Configure>', _on_report_config)

    # Ensure the inner frame expands to the canvas width to avoid empty right-side area
    def _on_canvas_config(event):
        try:
            report_canvas.itemconfig(report_window, width=event.width)
        except Exception:
            pass
    report_canvas.bind('<Configure>', _on_canvas_config)

    # mouse wheel scrolling (Windows). Bind to canvas for wheel, but use generic binding for app.
    def _on_mousewheel(event):
        try:
            # event.delta is multiple of 120 on Windows
            report_canvas.yview_scroll(int(-1*(event.delta/120)), 'units')
        except Exception:
            try:
                # For other systems (Linux) use event.num
                if event.num == 4:
                    report_canvas.yview_scroll(-1, 'units')
                elif event.num == 5:
                    report_canvas.yview_scroll(1, 'units')
            except Exception:
                pass
    # Bind wheel events
    report_canvas.bind_all('<MouseWheel>', _on_mousewheel)
    report_canvas.bind_all('<Button-4>', _on_mousewheel)
    report_canvas.bind_all('<Button-5>', _on_mousewheel)
    ttk.Label(report_frame, text="Export Data to Excel", font=("Segoe UI", 13, "bold")).pack(pady=(0, 10))
    # Export mode selection
    export_mode = tk.StringVar(value="date")
    mode_frame = ttk.Frame(report_frame)
    mode_frame.pack(pady=5)
    ttk.Radiobutton(mode_frame, text="By Date", variable=export_mode, value="date").pack(side='left', padx=5)
    ttk.Radiobutton(mode_frame, text="By Month", variable=export_mode, value="month").pack(side='left', padx=5)
    ttk.Radiobutton(mode_frame, text="By Month Range", variable=export_mode, value="range").pack(side='left', padx=5)
    # Date pickers (use yyyy-mm-dd for all, extract year/month as needed)
    date_picker = DateEntry(report_frame, width=12, date_pattern='yyyy-mm-dd')
    month_picker = DateEntry(report_frame, width=12, date_pattern='yyyy-mm-dd')
    from_month_picker = DateEntry(report_frame, width=12, date_pattern='yyyy-mm-dd')
    to_month_picker = DateEntry(report_frame, width=12, date_pattern='yyyy-mm-dd')
    # Placeholders for dynamic widgets
    picker_widgets = [date_picker, month_picker, from_month_picker, to_month_picker]
    def show_picker(*args):
        # Remove all previous widgets and labels
        for w in picker_widgets:
            w.pack_forget()
        # Remove all labels after the export_mode radio buttons
        for child in report_frame.pack_slaves():
            if isinstance(child, ttk.Label) and child != report_frame.winfo_children()[0]:
                child.pack_forget()
        if export_mode.get() == "date":
            ttk.Label(report_frame, text="Select end date:").pack()
            date_picker.pack(pady=5)
            try:
                date_picker.bind('<<DateEntrySelected>>', lambda e: draw_chart())
            except Exception:
                try:
                    date_picker.bind('<<ComboboxSelected>>', lambda e: draw_chart())
                except Exception:
                    date_picker.bind('<FocusOut>', lambda e: draw_chart())
        elif export_mode.get() == "month":
            ttk.Label(report_frame, text="Select month:").pack()
            month_picker.pack(pady=5)
            try:
                month_picker.bind('<<DateEntrySelected>>', lambda e: draw_chart())
            except Exception:
                try:
                    month_picker.bind('<<ComboboxSelected>>', lambda e: draw_chart())
                except Exception:
                    month_picker.bind('<FocusOut>', lambda e: draw_chart())
        elif export_mode.get() == "range":
            ttk.Label(report_frame, text="From month:").pack()
            from_month_picker.pack(pady=2)
            try:
                from_month_picker.bind('<<DateEntrySelected>>', lambda e: draw_chart())
            except Exception:
                try:
                    from_month_picker.bind('<<ComboboxSelected>>', lambda e: draw_chart())
                except Exception:
                    from_month_picker.bind('<FocusOut>', lambda e: draw_chart())
            ttk.Label(report_frame, text="To month:").pack()
            to_month_picker.pack(pady=2)
            try:
                to_month_picker.bind('<<DateEntrySelected>>', lambda e: draw_chart())
            except Exception:
                try:
                    to_month_picker.bind('<<ComboboxSelected>>', lambda e: draw_chart())
                except Exception:
                    to_month_picker.bind('<FocusOut>', lambda e: draw_chart())
    export_mode.trace_add('write', show_picker)
    show_picker()
    # Export logic
    def export_report():
        mode = export_mode.get()
        if mode == "date":
            end_date = pd.to_datetime(date_picker.get_date()).replace(tzinfo=None)
            def in_range(val):
                d = pd.to_datetime(val, errors='coerce').replace(tzinfo=None)
                return d <= end_date
            date_str = end_date.strftime('%B_%d_%Y')
        elif mode == "month":
            m = pd.to_datetime(month_picker.get_date(), errors='coerce')
            start = m.replace(day=1, tzinfo=None)
            from calendar import monthrange
            end = m.replace(day=monthrange(m.year, m.month)[1], tzinfo=None)
            def in_range(val):
                d = pd.to_datetime(val, errors='coerce').replace(tzinfo=None)
                return start <= d <= end
            date_str = m.strftime('%B_%Y')
        # Purchases
        purchases = list(purchases_collection.find())
        purchases_df = pd.DataFrame([
            {
                "Item": p.get("item", p.get("Item", "")),
                "Quantity": p.get("quantity", p.get("Quantity", "")),
                "Price": p.get("unitPrice", p.get("Price", "")),
                "Total": p.get("total", p.get("Total", "")),
                "Date": p.get("date", p.get("Date", ""))
            }
            for p in purchases
            if (p.get("isDelete", False) is False) and (('date' in p or 'Date' in p) and in_range(p.get('date', p.get('Date'))))
        ])

        # Sales
        sales = list(sales_collection.find())
        sales_df = pd.DataFrame([
            {
                "Product": s.get("productId", s.get("Product", "")),
                "Quantity": s.get("quantitySold", s.get("Quantity", "")),
                "Price": s.get("sellingPrice", s.get("Price", "")),
                "Total": s.get("total", s.get("Total", "")),
                "Date": s.get("date", s.get("Date", ""))
            }
            for s in sales
            if (s.get("isDelete", False) is False) and (('date' in s or 'Date' in s) and in_range(s.get('date', s.get('Date'))))
        ])

        # Inventory (as of end)
        inventory = {}
        for p in purchases:
            if p.get("isDelete", False) is False:
                d = pd.to_datetime(p.get('date', p.get('Date')), errors='coerce').replace(tzinfo=None)
                if in_range(d):
                    item = p.get("item", p.get("Item", ""))
                    qty = int(p.get("quantity", p.get("Quantity", 0)))
                    inventory[item] = inventory.get(item, 0) + qty
        for s in sales:
            if s.get("isDelete", False) is False:
                d = pd.to_datetime(s.get('date', s.get('Date')), errors='coerce').replace(tzinfo=None)
                if in_range(d):
                    item = s.get("productId", s.get("Product", ""))
                    qty = int(s.get("quantitySold", s.get("Quantity", 0)))
                    inventory[item] = inventory.get(item, 0) - qty
        # Add snapshot date to inventory_df
        snapshot_date = None
        if mode == "date":
            snapshot_date = end_date.strftime('%Y-%m-%d')
        elif mode == "month":
            snapshot_date = end.strftime('%Y-%m-%d')
        inventory_df = pd.DataFrame([
            {"Item": k, "Quantity": v, "Date": snapshot_date} for k, v in inventory.items()
        ])

    

        # Format date columns to strip after T
        def strip_time(val):
            if isinstance(val, str) and 'T' in val:
                return val.split('T')[0] + ' ' + val.split('T')[1][:5]
            return val
        if not purchases_df.empty:
            purchases_df["Date"] = purchases_df["Date"].apply(strip_time)
        if not sales_df.empty:
            sales_df["Date"] = sales_df["Date"].apply(strip_time)
        if not inventory_df.empty and "Date" in inventory_df.columns:
            inventory_df["Date"] = inventory_df["Date"].apply(strip_time)

        # Save dialog
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=f"{date_str}.xlsx",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if not file_path:
            return
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            purchases_df.to_excel(writer, sheet_name="Purchases", index=False)
            sales_df.to_excel(writer, sheet_name="Sales", index=False)
            inventory_df.to_excel(writer, sheet_name="Inventory", index=False)
            # Format tables
            inventory_df["Date"] = inventory_df["Date"].apply(strip_time)
        # Save dialog
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=f"{date_str}.xlsx",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if not file_path:
            return
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            purchases_df.to_excel(writer, sheet_name="Purchases", index=False)
            sales_df.to_excel(writer, sheet_name="Sales", index=False)
            inventory_df.to_excel(writer, sheet_name="Inventory", index=False)
            # Format tables
            for sheet, df in zip(["Purchases", "Sales", "Inventory"], [purchases_df, sales_df, inventory_df]):
                worksheet = writer.sheets[sheet]
                for i, col in enumerate(df.columns):
                    max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                    worksheet.set_column(i, i, max_len)
    ttk.Button(report_frame, text="Export to Excel", command=export_report, style='TButton').pack(pady=10)

    # --- Charts area (bar / pie) ---
    charts_frame = ttk.LabelFrame(report_frame, text="Charts", padding=8)
    charts_frame.pack(fill='both', expand=True, pady=(12,0))

    chart_controls = ttk.Frame(charts_frame)
    chart_controls.pack(fill='x', pady=(0,8))
    chart_type_var = tk.StringVar(value="Top Sellers (Bar)")
    chart_choices = [
        "Top Sellers (Bar)",
        "Sales vs Purchases (Pie)",
        "Monthly Sales vs Purchases (Double Bar)",
        "Item Sales by Month (Pie)"
    ]
    ttk.Label(chart_controls, text="Chart:").pack(side='left', padx=(0,6))
    chart_combo = ttk.Combobox(chart_controls, textvariable=chart_type_var, values=chart_choices, state='readonly', width=24)
    chart_combo.pack(side='left')
    # Item selector (hidden unless user selects Item Sales by Month)
    item_var = tk.StringVar()
    item_combo = ttk.Combobox(chart_controls, textvariable=item_var, values=[], state='readonly', width=30)
    item_combo.pack(side='left', padx=(8,0))
    item_combo.pack_forget()

    # Auto-refresh bindings: when chart selection or item selection changes, redraw
    try:
        chart_type_var.trace_add('write', lambda *a: draw_chart())
    except Exception:
        try:
            chart_type_var.trace('w', lambda *a: draw_chart())
        except Exception:
            chart_combo.bind('<<ComboboxSelected>>', lambda e: draw_chart())
    try:
        item_var.trace_add('write', lambda *a: draw_chart())
    except Exception:
        try:
            item_var.trace('w', lambda *a: draw_chart())
        except Exception:
            item_combo.bind('<<ComboboxSelected>>', lambda e: draw_chart())

    def _populate_item_list():
        try:
            items_set = set()
            # include inventory items (from computed inventory snapshot)
            try:
                inv_data = get_inventory_data()
                for d in inv_data:
                    itm = d.get('Item')
                    if itm:
                        items_set.add(str(itm))
            except Exception:
                pass
            # include purchases items
            try:
                for p in purchases_collection.find():
                    if p.get('isDelete', False) is False:
                        itm = p.get('item', p.get('Item', ''))
                        if itm:
                            items_set.add(str(itm))
            except Exception:
                pass
            # include sales products
            try:
                for s in sales_collection.find():
                    if s.get('isDelete', False) is False:
                        val = s.get('productId', s.get('Product', ''))
                        if val:
                            items_set.add(str(val))
            except Exception:
                pass

            items = sorted(items_set)
            item_combo.config(values=items)
            if items:
                item_var.set(items[0])
        except Exception:
            item_combo.config(values=[])

    def _on_chart_choice_change(*a):
        # show/hide item selector depending on chart choice
        if chart_type_var.get() == 'Item Sales by Month (Pie)':
            try:
                _populate_item_list()
            except Exception:
                pass
            item_combo.pack(side='left', padx=(8,0))
            try:
                draw_chart()
            except Exception:
                pass
        else:
            try:
                item_combo.pack_forget()
            except Exception:
                pass

    # Bind change to show/hide item selector
    try:
        chart_type_var.trace_add('write', lambda *a: _on_chart_choice_change())
    except Exception:
        try:
            chart_type_var.trace('w', lambda *a: _on_chart_choice_change())
        except Exception:
            chart_combo.bind('<<ComboboxSelected>>', lambda e: _on_chart_choice_change())

    chart_area = ttk.Frame(charts_frame)
    chart_area.pack(fill='both', expand=True)

    # Lazy import matplotlib; show helpful message if missing
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        _MATPLOTLIB_OK = True
    except Exception:
        _MATPLOTLIB_OK = False
        ttk.Label(chart_area, text="Install `matplotlib` to enable charts (pip install matplotlib)").pack(padx=8, pady=8)

    def _gather_report_dfs_for_charts():
        # Duplicate the export range logic to produce sales and purchases DataFrames
        mode = export_mode.get()
        try:
            if mode == "date":
                end_date = pd.to_datetime(date_picker.get_date()).replace(tzinfo=None)
                def in_range(val):
                    d = pd.to_datetime(val, errors='coerce').replace(tzinfo=None)
                    return d <= end_date
            elif mode == "month":
                m = pd.to_datetime(month_picker.get_date(), errors='coerce')
                start = m.replace(day=1, tzinfo=None)
                from calendar import monthrange
                end = m.replace(day=monthrange(m.year, m.month)[1], tzinfo=None)
                def in_range(val):
                    d = pd.to_datetime(val, errors='coerce').replace(tzinfo=None)
                    return start <= d <= end
            else:
                fm = pd.to_datetime(from_month_picker.get_date(), errors='coerce')
                tm = pd.to_datetime(to_month_picker.get_date(), errors='coerce')
                if fm is None or tm is None:
                    def in_range(val):
                        return True
                else:
                    start = fm.replace(day=1, tzinfo=None)
                    from calendar import monthrange
                    end = tm.replace(day=monthrange(tm.year, tm.month)[1], tzinfo=None)
                    def in_range(val):
                        d = pd.to_datetime(val, errors='coerce').replace(tzinfo=None)
                        return start <= d <= end
        except Exception:
            def in_range(val):
                return True

        purchases = list(purchases_collection.find())
        purchases_df = pd.DataFrame([
            {
                "Item": p.get("item", p.get("Item", "")),
                "Quantity": p.get("quantity", p.get("Quantity", "")),
                "Price": p.get("unitPrice", p.get("Price", "")),
                "Total": p.get("total", p.get("Total", "")),
                "Date": p.get("date", p.get("Date", ""))
            }
            for p in purchases
            if (p.get("isDelete", False) is False) and (('date' in p or 'Date' in p) and in_range(p.get('date', p.get('Date'))))
        ])

        sales = list(sales_collection.find())
        sales_df = pd.DataFrame([
            {
                "Product": s.get("productId", s.get("Product", "")),
                "Quantity": s.get("quantitySold", s.get("Quantity", "")),
                "Price": s.get("sellingPrice", s.get("Price", "")),
                "Total": s.get("total", s.get("Total", "")),
                "Date": s.get("date", s.get("Date", ""))
            }
            for s in sales
            if (s.get("isDelete", False) is False) and (('date' in s or 'Date' in s) and in_range(s.get('date', s.get('Date'))))
        ])
        return purchases_df, sales_df

    _current_canvas = None
    def _clear_canvas_area():
        nonlocal _current_canvas
        for c in chart_area.winfo_children():
            c.destroy()
        _current_canvas = None

    def draw_chart():
        nonlocal _current_canvas
        _clear_canvas_area()
        if not _MATPLOTLIB_OK:
            ttk.Label(chart_area, text="matplotlib not available").pack()
            return
        purchases_df, sales_df = _gather_report_dfs_for_charts()
        choice = chart_type_var.get()
        fig = matplotlib.figure.Figure(figsize=(6,4), dpi=100)
        ax = fig.add_subplot(111)
        try:
            if choice == "Top Sellers (Bar)":
                if sales_df.empty:
                    ax.text(0.5, 0.5, 'No sales data for selected range', ha='center', va='center')
                else:
                    # aggregate by quantity sold (units) instead of revenue
                    grp = sales_df.groupby('Product')['Quantity'].apply(lambda s: pd.to_numeric(s, errors='coerce').fillna(0).astype(float).sum())
                    grp = grp.sort_values(ascending=False).head(10)
                    labels = grp.index.astype(str)
                    positions = list(range(len(labels)))
                    ax.bar(positions, grp.values)
                    ax.set_xticks(positions)
                    ax.set_xticklabels(labels, rotation=30, ha='right')
                    ax.set_ylabel('Units Sold')
                    ax.set_title('Top Sellers by Units Sold')
                    try:
                        fig.tight_layout()
                    except Exception:
                        pass
            elif choice == "Sales vs Purchases (Pie)":
                total_sales = pd.to_numeric(sales_df['Total'], errors='coerce').fillna(0).astype(float).sum() if not sales_df.empty else 0.0
                total_purchases = pd.to_numeric(purchases_df['Total'], errors='coerce').fillna(0).astype(float).sum() if not purchases_df.empty else 0.0
                sizes = [total_sales, total_purchases]
                labels = [f"Sales ({total_sales:,.2f})", f"Purchases ({total_purchases:,.2f})"]
                if sum(sizes) == 0:
                    ax.text(0.5, 0.5, 'No sales or purchases data for selected range', ha='center', va='center')
                else:
                    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
                    ax.axis('equal')
                    ax.set_title('Sales vs Purchases')
            elif choice == "Monthly Sales vs Purchases (Double Bar)":
                # aggregate totals per month (year-month) for both sales and purchases
                import numpy as _np
                try:
                    if not sales_df.empty and 'Total' in sales_df.columns:
                        sales_df['Date'] = pd.to_datetime(sales_df['Date'], errors='coerce')
                        sales_df['YM'] = sales_df['Date'].dt.to_period('M').astype(str)
                        sales_month = pd.to_numeric(sales_df['Total'], errors='coerce').fillna(0).astype(float).groupby(sales_df['YM']).sum()
                    else:
                        sales_month = pd.Series(dtype=float)
                except Exception:
                    sales_month = pd.Series(dtype=float)
                try:
                    if not purchases_df.empty and 'Total' in purchases_df.columns:
                        purchases_df['Date'] = pd.to_datetime(purchases_df['Date'], errors='coerce')
                        purchases_df['YM'] = purchases_df['Date'].dt.to_period('M').astype(str)
                        purchases_month = pd.to_numeric(purchases_df['Total'], errors='coerce').fillna(0).astype(float).groupby(purchases_df['YM']).sum()
                    else:
                        purchases_month = pd.Series(dtype=float)
                except Exception:
                    purchases_month = pd.Series(dtype=float)

                # union of months (sorted chronologically)
                try:
                    months = sorted(set(list(sales_month.index) + list(purchases_month.index)), key=lambda s: pd.Period(s, freq='M'))
                except Exception:
                    months = sorted(set(list(sales_month.index) + list(purchases_month.index)))

                if not months:
                    ax.text(0.5, 0.5, 'No data for selected range', ha='center', va='center')
                else:
                    sales_vals = [float(sales_month.get(m, 0.0)) for m in months]
                    purchases_vals = [float(purchases_month.get(m, 0.0)) for m in months]
                    x = _np.arange(len(months))
                    width = 0.35
                    ax.bar(x - width/2, sales_vals, width, label='Sales')
                    ax.bar(x + width/2, purchases_vals, width, label='Purchases')
                    ax.set_xticks(x)
                    # smaller font and rotated labels to fit more months; ensure layout fits
                    ax.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
                    ax.set_ylabel('Total')
                    ax.set_title('Monthly Sales vs Purchases')
                    ax.legend()
                    try:
                        # expand figure width based on number of months so labels are readable
                        fig_width = max(6, len(months) * 0.6)
                        fig.set_size_inches(fig_width, 4)
                    except Exception:
                        pass
                    try:
                        fig.tight_layout()
                    except Exception:
                        pass
            elif choice == "Item Sales by Month (Pie)":
                # Pie chart: distribution of sales for a selected item across months
                selected = item_var.get()
                if not selected:
                    ax.text(0.5, 0.5, 'No item selected', ha='center', va='center')
                else:
                    try:
                        df = sales_df.copy()
                        if df.empty or 'Product' not in df.columns:
                            ax.text(0.5, 0.5, 'No sales data for selected range', ha='center', va='center')
                        else:
                            df = df[df['Product'].astype(str) == str(selected)]
                            if df.empty:
                                ax.text(0.5, 0.5, 'No sales for selected item in range', ha='center', va='center')
                            else:
                                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                                df['YM'] = df['Date'].dt.to_period('M').astype(str)
                                grp = pd.to_numeric(df['Total'], errors='coerce').fillna(0).astype(float).groupby(df['YM']).sum()
                                if grp.empty:
                                    ax.text(0.5, 0.5, 'No sales for selected item in range', ha='center', va='center')
                                else:
                                    labels = grp.index.astype(str)
                                    sizes = grp.values
                                    try:
                                        total = float(getattr(sizes, 'sum', lambda: sum(sizes))())
                                    except Exception:
                                        try:
                                            total = float(sum(sizes))
                                        except Exception:
                                            total = 0.0
                                    def _abs_autopct(pct):
                                        try:
                                            val = int(round(pct / 100.0 * total))
                                            return f"{val:,}"
                                        except Exception:
                                            return ''
                                    ax.pie(sizes, labels=labels, autopct=_abs_autopct, startangle=90)
                                    ax.axis('equal')
                                    ax.set_title(f'Sales distribution for {selected}')
                    except Exception as e:
                        ax.text(0.5, 0.5, f'Error preparing item chart: {e}', ha='center', va='center')
        except Exception as e:
            ax.text(0.5, 0.5, f'Error plotting: {e}', ha='center', va='center')

        canvas = FigureCanvasTkAgg(fig, master=chart_area)
        canvas.draw()
        widget = canvas.get_tk_widget()
        widget.pack(fill='both', expand=True)
        _current_canvas = canvas

    # initial chart draw
    try:
        draw_chart()
    except Exception:
        pass

    # --- DASHBOARD TAB ---
    # Create a simple dashboard summarizing key metrics
    dashboard_tab = tabs.get("Dashboard")
    if dashboard_tab is not None:
        dash_frame = ttk.Frame(dashboard_tab, padding=12)
        dash_frame.pack(fill='both', expand=True)

    # Dashboard metric cards: daily & monthly summaries
    # Local styles for dashboard cards
    style.configure('DashCardTitle.TLabel', font=('Segoe UI', 10), foreground=malibu['800'], background=malibu['50'])
    style.configure('DashCardValue.TLabel', font=('Segoe UI', 14, 'bold'), foreground=malibu['900'], background=malibu['50'])

    # StringVars for metrics (kept same names for compatibility)
    daily_sold_var = tk.StringVar(value="Daily Sold Items: 0")
    daily_purchases_var = tk.StringVar(value="Daily Purchases: 0")
    current_stock_var = tk.StringVar(value="Current Stock: 0")
    monthly_revenue_var = tk.StringVar(value="Monthly Revenue: 0.00")
    monthly_spent_var = tk.StringVar(value="Monthly Spent: 0.00")
    monthly_profit_var = tk.StringVar(value="Monthly Profit: 0.00")

    metrics_container = ttk.Frame(dash_frame)
    metrics_container.pack(fill='x', pady=(0, 10))

    metrics = [
        ("Daily Sold Items", daily_sold_var),
        ("Daily Purchases", daily_purchases_var),
        ("Current Stock", current_stock_var),
        ("Monthly Revenue", monthly_revenue_var),
        ("Monthly Spent", monthly_spent_var),
        ("Monthly Profit", monthly_profit_var),
    ]

    # Create 2 rows x 3 columns of card-like panels
    for idx, (title, var) in enumerate(metrics):
        card = tk.Frame(metrics_container, bg=malibu['50'], bd=1, relief='solid')
        card.grid(row=idx // 3, column=idx % 3, padx=8, pady=6, sticky='nsew')
        metrics_container.grid_columnconfigure(idx % 3, weight=1)
        ttk.Label(card, text=title, style='DashCardTitle.TLabel').pack(anchor='w', padx=10, pady=(8, 0))
        ttk.Label(card, textvariable=var, style='DashCardValue.TLabel').pack(anchor='w', padx=10, pady=(4, 10))

    # Small separator
    ttk.Separator(dash_frame, orient='horizontal').pack(fill='x', pady=(4, 8))

    # Top summary labels (Revenue, Cost, Profit) below cards for quick reference
    summary_frame = ttk.Frame(dash_frame)
    summary_frame.pack(fill='x', pady=(0, 8))
    total_revenue_var = tk.StringVar(value="Total Revenue: 0.00")
    total_cost_var = tk.StringVar(value="Total Cost: 0.00")
    total_profit_var = tk.StringVar(value="Actual Profit: 0.00")
    ttk.Label(summary_frame, textvariable=total_revenue_var, font=("Segoe UI", 11, 'bold')).pack(side='left', padx=8)
    ttk.Label(summary_frame, textvariable=total_cost_var, font=("Segoe UI", 11, 'bold')).pack(side='left', padx=8)
    # Pack Actual Profit on the left and put pickers to its right inside a picker_frame
    ttk.Label(summary_frame, textvariable=total_profit_var, font=("Segoe UI", 11, 'bold')).pack(side='left', padx=8)
    # Picker frame next to Actual Profit for visibility
    picker_frame = ttk.Frame(summary_frame)
    picker_frame.pack(side='left', padx=(12,0))
    # Use comboboxes for start/end day, month, year to select a date range
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    try:
        import datetime as _dt
        current_year = _dt.datetime.now().year
    except Exception:
        current_year = 2025
    years = [str(y) for y in range(current_year-5, current_year+3)]

    # Start range vars
    start_day_var = tk.StringVar()
    start_month_var = tk.StringVar()
    start_year_var = tk.StringVar()
    # End range vars
    end_day_var = tk.StringVar()
    end_month_var = tk.StringVar()
    end_year_var = tk.StringVar()

    ttk.Label(picker_frame, text="From:").pack(side='left', padx=(0,4))
    start_day_combo = ttk.Combobox(picker_frame, textvariable=start_day_var, values=[], width=4, state='readonly')
    start_day_combo.pack(side='left')
    start_month_combo = ttk.Combobox(picker_frame, textvariable=start_month_var, values=month_names, width=5, state='readonly')
    start_month_combo.pack(side='left', padx=(4,0))
    start_year_combo = ttk.Combobox(picker_frame, textvariable=start_year_var, values=years, width=6, state='readonly')
    start_year_combo.pack(side='left', padx=(4,8))

    ttk.Label(picker_frame, text="To:").pack(side='left', padx=(8,4))
    end_day_combo = ttk.Combobox(picker_frame, textvariable=end_day_var, values=[], width=4, state='readonly')
    end_day_combo.pack(side='left')
    end_month_combo = ttk.Combobox(picker_frame, textvariable=end_month_var, values=month_names, width=5, state='readonly')
    end_month_combo.pack(side='left', padx=(4,0))
    end_year_combo = ttk.Combobox(picker_frame, textvariable=end_year_var, values=years, width=6, state='readonly')
    end_year_combo.pack(side='left', padx=(4,8))

    # default selection: start = first of current month, end = today
    try:
        _now = datetime.now()
        start_month_var.set(month_names[_now.month-1])
        start_year_var.set(str(_now.year))
        start_day_var.set('1')
        end_month_var.set(month_names[_now.month-1])
        end_year_var.set(str(_now.year))
        end_day_var.set(str(_now.day))
    except Exception:
        try:
            start_month_var.set(month_names[0])
            start_year_var.set(years[-1])
            start_day_var.set('1')
            end_month_var.set(month_names[0])
            end_year_var.set(years[-1])
            end_day_var.set('1')
        except Exception:
            pass

    # helper to update day list for a given combo based on month/year vars
    def _update_days_for(day_combo, day_var, month_var_local, year_var_local):
        try:
            import calendar as _cal
            m = month_names.index(str(month_var_local.get())) + 1
            y = int(year_var_local.get())
            days_in_month = _cal.monthrange(y, m)[1]
            days = [str(d) for d in range(1, days_in_month+1)]
            cur = day_var.get()
            day_combo.config(values=days)
            if cur and cur in days:
                day_var.set(cur)
            else:
                try:
                    td = datetime.now().day
                    if str(td) in days:
                        day_var.set(str(td))
                    else:
                        day_var.set(days[0])
                except Exception:
                    day_var.set(days[0] if days else '1')
        except Exception:
            day_combo.config(values=[str(i) for i in range(1,32)])
            if not day_var.get():
                day_var.set('1')

    # update both start and end days
    def update_days():
        _update_days_for(start_day_combo, start_day_var, start_month_var, start_year_var)
        _update_days_for(end_day_combo, end_day_var, end_month_var, end_year_var)

    # auto-refresh when range changes
    def _on_range_change(*a):
        update_days()
        refresh_dashboard()

    # Trace registrations for start/end range will be attached after
    # `refresh_dashboard` is defined to avoid referencing it before assignment.
    update_days()

    # Make sure combobox user selections always trigger refresh (fallback)
    try:
        start_month_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
        start_year_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
        end_month_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
        end_year_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
        start_day_combo.bind('<<ComboboxSelected>>', lambda e: refresh_dashboard())
        end_day_combo.bind('<<ComboboxSelected>>', lambda e: refresh_dashboard())
    except Exception:
        pass

    # Dashboard helper: low stock threshold (user-adjustable) and quantity formatter
    # Store threshold in a DoubleVar so the user can change it via an Entry widget
    low_stock_threshold_var = tk.DoubleVar(value=5.0)
    def format_qty(val):
        # """Format quantity: show as integer when whole, otherwise two decimals."""
        try:
            f = float(val)
            return str(int(f)) if f.is_integer() else f"{f:.2f}"
        except Exception:
            return str(val)

    # Middle: Top sellers and Low stock lists
    mid_frame = ttk.Frame(dash_frame)
    mid_frame.pack(fill='both', expand=True)

    left_col = ttk.Frame(mid_frame)
    left_col.pack(side='left', fill='both', expand=True, padx=(0,8))
    right_col = ttk.Frame(mid_frame)
    right_col.pack(side='left', fill='both', expand=True, padx=(8,0))

    # Left: Top Sellers card with Treeview
    left_card = ttk.LabelFrame(left_col, text="Top Sellers (by revenue)", padding=8)
    left_card.pack(fill='both', expand=True, pady=(0,0))
    # Top sellers with vertical scrollbar
    top_frame = ttk.Frame(left_card)
    top_frame.pack(fill='both', expand=True)
    top_tree = ttk.Treeview(top_frame, columns=("Item", "Revenue", "Qty"), show='headings', height=12)
    top_vsb = ttk.Scrollbar(top_frame, orient='vertical', command=top_tree.yview)
    top_tree.configure(yscrollcommand=top_vsb.set)
    for col in top_tree['columns']:
        top_tree.heading(col, text=col)
        top_tree.column(col, anchor='center', width=140)
    top_tree.pack(side='left', fill='both', expand=True)
    top_vsb.pack(side='right', fill='y')

    # Right: Low Stock card with Treeview
    # We'll render a header inside the card so we can place the threshold entry next to the title
    right_card = ttk.LabelFrame(right_col, text="", padding=8)
    right_card.pack(fill='both', expand=True, pady=(0,0))
    # Header inside the card (title + threshold entry)
    right_header = ttk.Frame(right_card)
    right_header.pack(fill='x', pady=(0,6))
    # Title label showing current threshold (updated in refresh_dashboard)
    right_header_label = ttk.Label(right_header, text=f"Low Stock (qty <= {int(low_stock_threshold_var.get())})", font=('Segoe UI', 10, 'bold'))
    right_header_label.pack(side='left')
    # Entry placed next to title for changing threshold
    right_header_entry = ttk.Entry(right_header, textvariable=low_stock_threshold_var, width=6)
    right_header_entry.pack(side='left', padx=(8,0))
    # Low stock with vertical scrollbar
    low_frame = ttk.Frame(right_card)
    low_frame.pack(fill='both', expand=True)
    low_tree = ttk.Treeview(low_frame, columns=("Item", "Qty"), show='headings', height=12)
    low_vsb = ttk.Scrollbar(low_frame, orient='vertical', command=low_tree.yview)
    low_tree.configure(yscrollcommand=low_vsb.set)
    for col in low_tree['columns']:
        low_tree.heading(col, text=col)
        low_tree.column(col, anchor='center', width=160)
    low_tree.pack(side='left', fill='both', expand=True)
    low_vsb.pack(side='right', fill='y')

    # Action row: only the Refresh button (pickers moved next to Actual Profit)
    action_frame = ttk.Frame(dash_frame)
    action_frame.pack(fill='x', pady=(8,0))
    ttk.Button(action_frame, text="Refresh Dashboard", command=lambda: refresh_dashboard()).pack(side='right')

    def refresh_dashboard():
        # Compute totals using non-deleted records
        total_revenue = 0.0
        total_cost = 0.0
        sold_by_item = {}
        qty_by_item = {}
        # New metrics
        from datetime import datetime
        now = datetime.now()
        today = now.date()
        # Determine selected start/end dates from range comboboxes; fall back to today on error
        try:
            s_day = int(start_day_var.get())
            s_month = month_names.index(str(start_month_var.get())) + 1
            s_year = int(start_year_var.get())
            e_day = int(end_day_var.get())
            e_month = month_names.index(str(end_month_var.get())) + 1
            e_year = int(end_year_var.get())
            start_date = datetime(s_year, s_month, s_day).date()
            end_date = datetime(e_year, e_month, e_day).date()
            if start_date > end_date:
                # swap so start <= end
                start_date, end_date = end_date, start_date
        except Exception:
            start_date = now.date()
            end_date = now.date()
        daily_sold_items = 0
        daily_purchases_items = 0
        monthly_revenue = 0.0
        monthly_spent = 0.0
        # Purchases
        for p in purchases_collection.find():
            if p.get('isDelete', False) is False:
                price = float(p.get('unitPrice', p.get('Price', 0)))
                qty = float(p.get('quantity', p.get('Quantity', 0)))
                total_cost += price * qty
                # Date-aware metrics
                date_val = p.get('date', p.get('Date', ''))
                try:
                    dt = datetime.fromisoformat(date_val)
                except Exception:
                    dt = None
                if dt is not None:
                    d = dt.date()
                    if start_date <= d <= end_date:
                        daily_purchases_items += int(qty)
                        monthly_spent += price * qty
        # Sales
        for s in sales_collection.find():
            if s.get('isDelete', False) is False:
                price = float(s.get('sellingPrice', s.get('Price', 0)))
                qty = float(s.get('quantitySold', s.get('Quantity', 0)))
                total_revenue += price * qty
                item = s.get('productId', s.get('Product', ''))
                sold_by_item[item] = sold_by_item.get(item, 0.0) + price * qty
                qty_by_item[item] = qty_by_item.get(item, 0.0) + qty
                # Date-aware metrics
                date_val = s.get('date', s.get('Date', ''))
                try:
                    dt = datetime.fromisoformat(date_val)
                except Exception:
                    dt = None
                if dt is not None:
                    d = dt.date()
                    if start_date <= d <= end_date:
                        daily_sold_items += int(qty)
                        monthly_revenue += price * qty

        # For cost of goods sold use average buy cost (same as inventory logic)
        from collections import defaultdict
        buy_qty = defaultdict(float)
        buy_totals = defaultdict(float)
        for p in purchases_collection.find():
            if p.get('isDelete', False) is False:
                item = p.get('item', p.get('Item', ''))
                qty = float(p.get('quantity', p.get('Quantity', 0)))
                price = float(p.get('unitPrice', p.get('Price', 0)))
                buy_qty[item] += qty
                buy_totals[item] += price * qty

        # Calculate actual profit from sold units
        actual_profit = 0.0
        for item, revenue in sold_by_item.items():
            sold_qty = qty_by_item.get(item, 0.0)
            avg_buy = (buy_totals.get(item, 0.0) / buy_qty.get(item, 1.0)) if buy_qty.get(item, 0.0) else 0.0
            cost_of_sold = avg_buy * sold_qty
            actual_profit += (revenue - cost_of_sold)

        # Update top sellers tree
        for r in top_tree.get_children():
            top_tree.delete(r)
        top_list = sorted(sold_by_item.items(), key=lambda kv: kv[1], reverse=True)[:10]
        for item, rev in top_list:
            top_tree.insert('', 'end', values=(item, f"{rev:,.2f}", int(qty_by_item.get(item, 0))))

        # Read threshold and update low stock tree using current inventory
        try:
            threshold = float(low_stock_threshold_var.get())
        except Exception:
            threshold = 1.0
        # Update header label to reflect current threshold
        try:
            display_thresh = int(threshold) if float(threshold).is_integer() else threshold
        except Exception:
            display_thresh = threshold
        try:
            right_header_label.config(text=f"Low Stock (qty <= {display_thresh})")
        except Exception:
            pass
        inv = {d['Item']: d['Quantity'] for d in get_inventory_data()}
        for r in low_tree.get_children():
            low_tree.delete(r)
        # Accept numeric quantities (int or float). Treat missing/non-numeric as skipped.
        low_items = []
        for it, q in inv.items():
            try:
                fq = float(q)
            except Exception:
                continue
            if fq <= threshold:
                low_items.append((it, fq))
        low_items.sort(key=lambda x: x[1])
        for it, q in low_items:
            low_tree.insert('', 'end', values=(it, format_qty(q)))

        # Current stock: sum of on-hand quantities (treat negative as 0 on-hand)
        try:
            # Sum on-hand quantities as floats (do not truncate fractional quantities)
            current_stock_value = sum(max(0.0, float(d.get('Quantity', d.get('quantity', 0)))) for d in get_inventory_data())
            current_stock = format_qty(current_stock_value)
        except Exception:
            current_stock = "0"

        # Monthly profit: simple revenue - spent for the current month
        monthly_profit = monthly_revenue - monthly_spent

        # Update newly added top metrics
        daily_sold_var.set(f"Daily Sold Items: {daily_sold_items}")
        daily_purchases_var.set(f"Daily Purchases: {daily_purchases_items}")
        current_stock_var.set(f"Current Stock: {current_stock}")
        monthly_revenue_var.set(f"Monthly Sale: {monthly_revenue:,.2f}")
        monthly_spent_var.set(f"Monthly Purchase: {monthly_spent:,.2f}")
        monthly_profit_var.set(f"Monthly Profit: {monthly_profit:,.2f}")

        # Update summary labels
        total_revenue_var.set(f"Total Sale: {total_revenue:,.2f}")
        total_cost_var.set(f"Total Purchase: {total_cost:,.2f}")
        total_profit_var.set(f"Actual Profit: {actual_profit:,.2f}")


    # WebSocket QR Tab
    # Auto-refresh dashboard when the low-stock threshold variable changes
    try:
        low_stock_threshold_var.trace_add('write', lambda *a: refresh_dashboard())
    except Exception:
        # Older Tk versions may not support trace_add; fallback to trace
        try:
            low_stock_threshold_var.trace('w', lambda *a: refresh_dashboard())
        except Exception:
            pass
        # Register traces for the date range comboboxes (attach inside this scope)
        try:
            start_month_var.trace_add('write', lambda *a: _on_range_change())
            start_year_var.trace_add('write', lambda *a: _on_range_change())
            end_month_var.trace_add('write', lambda *a: _on_range_change())
            end_year_var.trace_add('write', lambda *a: _on_range_change())
            start_day_var.trace_add('write', lambda *a: refresh_dashboard())
            end_day_var.trace_add('write', lambda *a: refresh_dashboard())
        except Exception:
            try:
                start_month_var.trace('w', lambda *a: _on_range_change())
                start_year_var.trace('w', lambda *a: _on_range_change())
                end_month_var.trace('w', lambda *a: _on_range_change())
                end_year_var.trace('w', lambda *a: _on_range_change())
                start_day_var.trace('w', lambda *a: refresh_dashboard())
                end_day_var.trace('w', lambda *a: refresh_dashboard())
            except Exception:
                start_month_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
                start_year_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
                end_month_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
                end_year_combo.bind('<<ComboboxSelected>>', lambda e: _on_range_change())
                start_day_combo.bind('<<ComboboxSelected>>', lambda e: refresh_dashboard())
                end_day_combo.bind('<<ComboboxSelected>>', lambda e: refresh_dashboard())

    ws_tab = tabs["WebSocket QR"]
    ws_url = f"ws://{get_local_ip()}:8765"
    qr_img = generate_qr_code(ws_url)
    buf = io.BytesIO()
    qr_img.save(buf, format='PNG')
    buf.seek(0)
    qr_img_tk = ImageTk.PhotoImage(Image.open(buf))
    qr_label = tk.Label(ws_tab, image=qr_img_tk)
    qr_label.image = qr_img_tk
    qr_label.pack(pady=20)
    url_label = tk.Label(ws_tab, text= "Scan this Qr Code to Connect To Phone App", font=("Arial", 12))
    url_label.pack(pady=10)

    msg_var = tk.StringVar()
    msg_label = tk.Label(ws_tab, textvariable=msg_var, font=("Arial", 10), fg="green")
    msg_label.pack(pady=10)

    def on_device_connect(msg):
        root.after(0, lambda: msg_var.set(msg))

    if ws_connect_callback:
        ws_connect_callback(on_device_connect)

    root.mainloop()

# --- Main Entry Point ---
if __name__ == "__main__":
    from tkinter import messagebox
    from shop_sys import set_ws_connect_callback, build_gui, start_ws_server

    def on_connect_msg(msg):
        pass  # Removed console print

    set_ws_connect_callback(on_connect_msg)  # Register FIRST
    threading.Thread(target=start_ws_server, args=(on_connect_msg,), daemon=True).start()  # Then start server
    build_gui()