import asyncio
import json
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Set
import os
from dotenv import load_dotenv
from datetime import datetime
import motor.motor_asyncio
from bson import ObjectId
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PostgreSQL connection for user data
def get_db_connection():
    conn = psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        dbname=os.getenv("dbname"),
        cursor_factory=RealDictCursor
    )
    return conn

# MongoDB connection for chat messages
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/chat")
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client.social_network_db
chat_collection = db.chat_messages

# Helper class for JSON serialization of MongoDB ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return super().default(o)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        # user_id -> WebSocket
        self.active_connections: Dict[int, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, user_id: int):
        await websocket.accept()
        self.active_connections[user_id] = websocket
    
    def disconnect(self, user_id: int):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
    
    async def send_personal_message(self, message: str, user_id: int):
        if user_id in self.active_connections:
            await self.active_connections[user_id].send_text(message)
            return True
        return False

manager = ConnectionManager()

# WebSocket endpoint for chat
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await manager.connect(websocket, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message_data = json.loads(data)
                receiver_id = message_data.get("receiver_id")
                message_content = message_data.get("message")
                
                if not receiver_id or not message_content:
                    await websocket.send_text(json.dumps({"error": "Invalid message format"}))
                    continue
                
                # Store message in MongoDB
                timestamp = datetime.utcnow()
                chat_message = {
                    "sender_id": user_id,
                    "receiver_id": receiver_id,
                    "message": message_content,
                    "timestamp": timestamp,
                    "is_read": False,
                    "conversation_id": sorted([user_id, receiver_id])  # For easier querying
                }
                
                result = await chat_collection.insert_one(chat_message)
                
                # Format message for sending
                formatted_message = {
                    "id": str(result.inserted_id),
                    "sender_id": user_id,
                    "message": message_content,
                    "timestamp": timestamp.isoformat(),
                    "is_read": False
                }
                
                # Send to receiver if online
                sent = await manager.send_personal_message(
                    json.dumps(formatted_message, cls=JSONEncoder), 
                    receiver_id
                )
                
                # Send confirmation to sender
                await websocket.send_text(json.dumps({
                    **formatted_message,
                    "delivered": sent
                }, cls=JSONEncoder))
                
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "Invalid JSON"}))
                
    except WebSocketDisconnect:
        manager.disconnect(user_id)

# REST API endpoint to get chat history
@app.get("/chat-history/{user_id1}/{user_id2}")
async def get_chat_history(user_id1: int, user_id2: int, limit: int = 50, skip: int = 0):
    # Convert string IDs to integers for comparison
    user_id1, user_id2 = int(user_id1), int(user_id2)
    
    # Query MongoDB for messages between these users
    cursor = chat_collection.find({
        "conversation_id": sorted([user_id1, user_id2])
    }).sort("timestamp", 1).skip(skip).limit(limit)
    
    messages = await cursor.to_list(length=limit)
    
    # Format the results
    result = []
    for msg in messages:
        msg["id"] = str(msg.pop("_id"))
        msg["timestamp"] = msg["timestamp"].isoformat()
        result.append(msg)
    
    return result

# Mark messages as read
@app.post("/mark-messages-read/{sender_id}/{receiver_id}")
async def mark_messages_read(sender_id: int, receiver_id: int):
    result = await chat_collection.update_many(
        {
            "sender_id": int(sender_id),
            "receiver_id": int(receiver_id),
            "is_read": False
        },
        {"$set": {"is_read": True}}
    )
    
    return {"marked_as_read": result.modified_count}

# Get unread message count
@app.get("/unread-messages/{user_id}")
async def get_unread_messages(user_id: int):
    pipeline = [
        {
            "$match": {
                "receiver_id": int(user_id),
                "is_read": False
            }
        },
        {
            "$group": {
                "_id": "$sender_id",
                "count": {"$sum": 1}
            }
        }
    ]
    
    results = await chat_collection.aggregate(pipeline).to_list(length=100)
    return {str(result["_id"]): result["count"] for result in results}

# Get recent conversations
@app.get("/recent-conversations/{user_id}")
async def get_recent_conversations(user_id: int, limit: int = 20):
    user_id = int(user_id)
    
    # Find all conversations where the user is involved
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"sender_id": user_id},
                    {"receiver_id": user_id}
                ]
            }
        },
        {
            "$sort": {"timestamp": -1}
        },
        {
            "$group": {
                "_id": {
                    "$cond": [
                        {"$eq": ["$sender_id", user_id]},
                        "$receiver_id",
                        "$sender_id"
                    ]
                },
                "last_message": {"$first": "$$ROOT"}
            }
        },
        {
            "$replaceRoot": {"newRoot": "$last_message"}
        },
        {
            "$sort": {"timestamp": -1}
        },
        {
            "$limit": limit
        }
    ]
    
    conversations = await chat_collection.aggregate(pipeline).to_list(length=limit)
    
    # Format the results
    result = []
    for conv in conversations:
        conv["id"] = str(conv.pop("_id"))
        conv["timestamp"] = conv["timestamp"].isoformat()
        
        # Determine the other user in the conversation
        other_user_id = conv["sender_id"] if conv["receiver_id"] == user_id else conv["receiver_id"]
        
        # Get unread count for this conversation
        unread_count = await chat_collection.count_documents({
            "sender_id": other_user_id,
            "receiver_id": user_id,
            "is_read": False
        })
        
        conv["unread_count"] = unread_count
        result.append(conv)
    
    return result

# Search messages
@app.get("/search-messages/{user_id}")
async def search_messages(user_id: int, query: str):
    user_id = int(user_id)
    
    # Search for messages containing the query string
    cursor = chat_collection.find({
        "$or": [
            {"sender_id": user_id},
            {"receiver_id": user_id}
        ],
        "message": {"$regex": query, "$options": "i"}  # Case-insensitive search
    }).sort("timestamp", -1).limit(50)
    
    messages = await cursor.to_list(length=50)
    
    # Format the results
    result = []
    for msg in messages:
        msg["id"] = str(msg.pop("_id"))
        msg["timestamp"] = msg["timestamp"].isoformat()
        result.append(msg)
    
    return result

# Delete a message
@app.delete("/message/{message_id}")
async def delete_message(message_id: str):
    try:
        result = await chat_collection.delete_one({"_id": ObjectId(message_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Message not found")
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

