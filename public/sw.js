self.addEventListener('push', function(e) {
  const data = e.data ? e.data.json() : {};
  e.waitUntil(
    self.registration.showNotification(data.title || '🎯 Sheets Allocated!', {
      body: data.body || 'Your sheets are approved and ready to download — tap to open.',
      icon: '/icon-192.png',
      badge: '/icon-192.png',
      tag: 'sheets-ready',
      renotify: true,
      data: data.data || {}
    })
  );
});

self.addEventListener('notificationclick', function(e) {
  e.notification.close();
  e.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(list) {
      for (const c of list) {
        if (c.url.includes(self.location.origin)) { c.focus(); return; }
      }
      return clients.openWindow('/');
    })
  );
});
