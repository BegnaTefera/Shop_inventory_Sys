# Shop Management System

A Python-based shop management desktop application designed to work seamlessly with a mobile application for managing sales and purchases in real time.

The system utilizes MongoDB for storage, WebSockets for device synchronization, and Tkinter for the desktop interface. A key highlight: the application automatically calculates inventory based on sales and purchase records, eliminating the need for a separate (and often redundant) inventory table.

## Features

### 🖥️ Desktop Management Interface
Built using Tkinter, allowing you to view and manage:
- **Sales & Purchase Records**: Full visibility of transactions
- **Inventory**: Real-time stock levels
- **Live Sync**: Instant updates from connected mobile devices

### 📱 Mobile App Connection
- **QR Code Pairing**: Generates a QR Code on the desktop to bridge the connection
- **WebSocket Sync**: The mobile app scans the code to connect to the server and automatically syncs shop data to the PC

### 📈 Inventory Calculation
Inventory is not stored as a static value. Instead, it is calculated dynamically to ensure 100% accuracy:
