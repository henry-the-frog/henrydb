// listen-notify.js — PostgreSQL-compatible LISTEN/NOTIFY event system
// Allows clients to subscribe to channels and receive notifications.
// LISTEN channel_name — subscribe
// NOTIFY channel_name, 'payload' — send notification to all listeners
// UNLISTEN channel_name — unsubscribe
// UNLISTEN * — unsubscribe from all

import { EventEmitter } from 'node:events';

/**
 * NotificationManager — manages LISTEN/NOTIFY channels.
 */
export class NotificationManager extends EventEmitter {
  constructor() {
    super();
    this._channels = new Map(); // channel → Set<listener_id>
    this._listeners = new Map(); // listener_id → Set<channel>
    this._queue = new Map(); // listener_id → [notifications]
    this._stats = {
      totalNotifications: 0,
      totalDeliveries: 0,
      totalListens: 0,
      totalUnlistens: 0,
    };
  }

  /**
   * Subscribe a listener to a channel.
   */
  listen(listenerId, channel) {
    const lowerChannel = channel.toLowerCase();

    if (!this._channels.has(lowerChannel)) {
      this._channels.set(lowerChannel, new Set());
    }
    this._channels.get(lowerChannel).add(listenerId);

    if (!this._listeners.has(listenerId)) {
      this._listeners.set(listenerId, new Set());
    }
    this._listeners.get(listenerId).add(lowerChannel);

    if (!this._queue.has(listenerId)) {
      this._queue.set(listenerId, []);
    }

    this._stats.totalListens++;
    this.emit('listen', { listenerId, channel: lowerChannel });
  }

  /**
   * Unsubscribe from a channel.
   */
  unlisten(listenerId, channel) {
    if (channel === '*') {
      // Unsubscribe from all channels
      const channels = this._listeners.get(listenerId);
      if (channels) {
        for (const ch of channels) {
          const subs = this._channels.get(ch);
          if (subs) {
            subs.delete(listenerId);
            if (subs.size === 0) this._channels.delete(ch);
          }
        }
      }
      this._listeners.delete(listenerId);
      this._stats.totalUnlistens++;
      return;
    }

    const lowerChannel = channel.toLowerCase();
    const subs = this._channels.get(lowerChannel);
    if (subs) {
      subs.delete(listenerId);
      if (subs.size === 0) this._channels.delete(lowerChannel);
    }

    const listenerChannels = this._listeners.get(listenerId);
    if (listenerChannels) {
      listenerChannels.delete(lowerChannel);
      if (listenerChannels.size === 0) this._listeners.delete(listenerId);
    }

    this._stats.totalUnlistens++;
  }

  /**
   * Send a notification to all listeners on a channel.
   * The sender does NOT receive their own notification (PostgreSQL behavior).
   */
  notify(senderId, channel, payload = '') {
    const lowerChannel = channel.toLowerCase();
    this._stats.totalNotifications++;

    const notification = {
      channel: lowerChannel,
      payload: String(payload),
      senderId,
      timestamp: Date.now(),
    };

    const subscribers = this._channels.get(lowerChannel);
    if (!subscribers || subscribers.size === 0) return 0;

    let deliveries = 0;
    for (const listenerId of subscribers) {
      // PostgreSQL: sender doesn't receive their own notification
      if (listenerId === senderId) continue;

      const queue = this._queue.get(listenerId);
      if (queue) {
        queue.push(notification);
        deliveries++;
        this.emit('notification', { listenerId, ...notification });
      }
    }

    this._stats.totalDeliveries += deliveries;
    return deliveries;
  }

  /**
   * Drain all pending notifications for a listener.
   * Returns array of notifications and clears the queue.
   */
  drain(listenerId) {
    const queue = this._queue.get(listenerId);
    if (!queue || queue.length === 0) return [];

    const notifications = [...queue];
    queue.length = 0;
    return notifications;
  }

  /**
   * Peek at pending notifications without draining.
   */
  peek(listenerId) {
    const queue = this._queue.get(listenerId);
    return queue ? [...queue] : [];
  }

  /**
   * Wait for the next notification (async).
   * Resolves when a notification is received or timeout expires.
   */
  waitForNotification(listenerId, timeoutMs = 5000) {
    // Check existing queue first
    const existing = this.drain(listenerId);
    if (existing.length > 0) return Promise.resolve(existing);

    return new Promise((resolve) => {
      const handler = (event) => {
        if (event.listenerId === listenerId) {
          this.removeListener('notification', handler);
          clearTimeout(timer);
          resolve(this.drain(listenerId));
        }
      };

      const timer = setTimeout(() => {
        this.removeListener('notification', handler);
        resolve([]); // timeout — no notifications
      }, timeoutMs);

      this.on('notification', handler);
    });
  }

  /**
   * Remove a listener entirely (disconnect cleanup).
   */
  removeListener_id(listenerId) {
    this.unlisten(listenerId, '*');
    this._queue.delete(listenerId);
  }

  /**
   * Get active channels and their subscriber counts.
   */
  getChannels() {
    const channels = [];
    for (const [name, subs] of this._channels) {
      channels.push({ channel: name, subscribers: subs.size });
    }
    return channels;
  }

  /**
   * Get channels a specific listener is subscribed to.
   */
  getListenerChannels(listenerId) {
    const channels = this._listeners.get(listenerId);
    return channels ? [...channels] : [];
  }

  getStats() {
    return {
      ...this._stats,
      activeChannels: this._channels.size,
      activeListeners: this._listeners.size,
      pendingNotifications: [...this._queue.values()].reduce((sum, q) => sum + q.length, 0),
    };
  }
}
