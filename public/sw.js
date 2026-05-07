self.addEventListener('push', function(e) {
  const data = e.data ? e.data.json() : {};
  e.waitUntil(
    self.registration.showNotification(data.title || 'Tungbola Market', {
      body: data.body || 'Your sheets are ready to download!',
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
  const token = e.notification.data && e.notification.data.downloadToken;
  const url = token ? '/?dl=' + token : '/';
  e.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(list) {
      for (const c of list) {
        if (c.url.includes(self.location.origin)) { c.focus(); c.navigate(url); return; }
      }
      return clients.openWindow(url);
    })
  );
});
