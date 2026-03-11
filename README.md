Shop Management SystemA Python-based shop management desktop application designed to work seamlessly with a mobile application for managing sales and purchases in real time.The system utilizes MongoDB for storage, WebSockets for device synchronization, and Tkinter for the desktop interface. A key highlight: the application automatically calculates inventory based on sales and purchase records, eliminating the need for a separate (and often redundant) inventory table.Features🖥️ Desktop Management InterfaceBuilt using Tkinter, allowing you to view and manage:Sales & Purchase Records: Full visibility of transactions.Inventory: Real-time stock levels.Live Sync: Instant updates from connected mobile devices.📱 Mobile App ConnectionQR Code Pairing: Generates a QR Code on the desktop to bridge the connection.WebSocket Sync: The mobile app scans the code to connect to the server and automatically syncs shop data to the PC.📈 Inventory CalculationInventory is not stored as a static value. Instead, it is calculated dynamically to ensure 100% accuracy:$$Inventory = \text{Total Purchases} - \text{Total Sales}$$📊 Monthly Analytics & ReportsPrice Tracking: Calculates average buying and selling prices.Product Stats: Monthly performance breakdown.Excel Export: Generate professional reports via Pandas for daily/monthly sales or custom date ranges.Date Filtering: Integrated tkcalendar for easy record navigation.System ArchitecturePlaintextMobile App
     │
     │ WebSocket
     ▼
Python WebSocket Server
     │
     ▼
MongoDB Database
     │
     ▼
Tkinter Desktop GUI
The mobile application pushes data to the WebSocket server, which commits it to MongoDB. The desktop GUI then pulls this data for real-time visualization.Technologies UsedTechnologyPurposePythonCore programming languageTkinterDesktop user interfaceMongoDBNoSQL DatabaseWebSocketsReal-time communicationQR CodeDevice pairingPandasReport generation & data manipulationPillowImage processingtkcalendarDate selection interfaceInstallation1. 
cd shop-management-system
1. Install DependenciesBashpip install tkinter pymongo websockets qrcode pillow pandas tkcalendar
2. Install MongoDBDownload and install MongoDB Community Server: LinkStart the MongoDB server:Bashmongod
Running the ApplicationRun the main Python script to launch the server and GUI:Bashpython shop_sys.py
Upon launch, the application will:Start the WebSocket server.Generate a connection QR code.Wait for the mobile app to sync.Populate the GUI with database records.Database Structuresales CollectionJSON{
  "productId": "Item001",
  "quantitySold": 2,
  "price": 50,
  "date": "2026-03-01",
  "last_update": "2026-03-01T12:30:00"
}
purchases CollectionJSON{
  "item": "Item001",
  "quantity": 10,
  "unitPrice": 30,
  "date": "2026-03-01",
  "last_update": "2026-03-01T10:00:00"
}
QR Code ConnectionThe desktop app generates a URI: ws://<LOCAL_IP>:<PORT>. Once the mobile app scans this, the handshake is completed and data flows bi-directionally.File StructurePlaintextproject/
│
├── shop_sys.py        # Main application logic & GUI
├── README.md          # Project documentation
└── requirements.txt   # (Recommended) List of dependencies
Future Improvements[ ] Authentication: Secure login for different roles.[ ] Cloud Support: Migration to MongoDB Atlas.[ ] Notifications: Telegram bot for low stock alerts.[ ] Scanning: Barcode/inventory scanning support via mobile camera.