const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    maxHttpBufferSize: 1e7
});

app.use(express.static(path.join(__dirname, 'public')));

const rooms = new Map();
const userToRoom = new Map();

io.on('connection', function(socket) {
    socket.on('create_room', function(username) {
        const roomId = crypto.randomBytes(4).toString('hex');
        rooms.set(roomId, {
            host: socket.id,
            members: new Map([[socket.id, username]]),
            pending: new Map()
        });
        userToRoom.set(socket.id, roomId);
        socket.join(roomId);
        socket.emit('room_created', { roomId: roomId, username: username });
    });

    socket.on('request_join', function(data) {
        const room = rooms.get(data.roomId);
        if (!room) {
            return socket.emit('join_error', 'Room does not exist or has been destroyed.');
        }
        room.pending.set(socket.id, data.username);
        io.to(room.host).emit('join_request', { socketId: socket.id, username: data.username });
        socket.emit('waiting_for_approval');
    });

    socket.on('approve_join', function(targetSocketId) {
        const roomId = userToRoom.get(socket.id);
        const room = rooms.get(roomId);
        if (room && room.host === socket.id && room.pending.has(targetSocketId)) {
            const username = room.pending.get(targetSocketId);
            room.pending.delete(targetSocketId);
            room.members.set(targetSocketId, username);
            userToRoom.set(targetSocketId, roomId);
            const targetSocket = io.sockets.sockets.get(targetSocketId);
            if (targetSocket) {
                targetSocket.join(roomId);
                targetSocket.emit('join_approved', { roomId: roomId, username: username, members: Array.from(room.members.values()) });
                socket.to(roomId).emit('system_message', username + ' joined the secure room.');
            }
        }
    });

    socket.on('reject_join', function(targetSocketId) {
        const roomId = userToRoom.get(socket.id);
        const room = rooms.get(roomId);
        if (room && room.host === socket.id && room.pending.has(targetSocketId)) {
            room.pending.delete(targetSocketId);
            const targetSocket = io.sockets.sockets.get(targetSocketId);
            if (targetSocket) {
                targetSocket.emit('join_rejected', 'The host rejected your join request.');
            }
        }
    });

    socket.on('send_message', function(text) {
        const roomId = userToRoom.get(socket.id);
        const room = rooms.get(roomId);
        if (room && room.members.has(socket.id)) {
            const username = room.members.get(socket.id);
            io.to(roomId).emit('receive_message', { username: username, text: text, type: 'text', senderId: socket.id });
        }
    });

    socket.on('send_voice', function(audioBuffer) {
        const roomId = userToRoom.get(socket.id);
        const room = rooms.get(roomId);
        if (room && room.members.has(socket.id)) {
            const username = room.members.get(socket.id);
            io.to(roomId).emit('receive_message', { username: username, audioBuffer: audioBuffer, type: 'audio', senderId: socket.id });
        }
    });

    socket.on('disconnect', function() {
        const roomId = userToRoom.get(socket.id);
        if (!roomId) return;
        const room = rooms.get(roomId);
        if (!room) return;

        if (room.host === socket.id) {
            socket.to(roomId).emit('room_destroyed', 'The host closed the room. All data has been purged.');
            room.members.forEach(function(_, memberSocketId) {
                const memberSocket = io.sockets.sockets.get(memberSocketId);
                if (memberSocket) memberSocket.leave(roomId);
                userToRoom.delete(memberSocketId);
            });
            rooms.delete(roomId);
        } else {
            const username = room.members.get(socket.id);
            room.members.delete(socket.id);
            userToRoom.delete(socket.id);
            socket.to(roomId).emit('system_message', username + ' has left the room.');
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
    console.log('[SECURE EPHEMERAL CHAT] Server running on http://localhost:' + PORT);
});