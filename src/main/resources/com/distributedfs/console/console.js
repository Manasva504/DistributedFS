// DistributedFS web console. Plain JavaScript, no libraries.
//
// Rules this file keeps (ConsoleAssetsTest checks them):
//  - every value from the API reaches the page as text (textContent) or as an attribute from a
//    short fixed list, never as HTML;
//  - styling is done with classes only, never with style attributes;
//  - it talks only to this server, with same-origin requests.
//
// Polling: GET /cluster/status every second; GET /files, GET /cluster/events and the selected
// file's GET /files/<id>/placement every 2 seconds and right after an upload or a repair.
// Nothing is polled while the tab is hidden.

(function () {
  'use strict';

  var STATUS_MS = 1000;
  var DATA_MS = 2000;
  var RETRY_MS = 5000;
  var MAX_COLUMNS = 64;
  var MAX_EVENTS_SHOWN = 30;
  var HASH_LIMIT_BYTES = 256 * 1024 * 1024;
  var ATTRIBUTES = ['class', 'role', 'href', 'download', 'title', 'aria-label', 'aria-pressed', 'aria-hidden',
    'aria-current', 'scope', 'type', 'datetime', 'viewBox', 'width', 'height', 'fill', 'stroke', 'stroke-width',
    'stroke-linecap', 'stroke-linejoin', 'd'];

  var state = {
    reachable: null,
    status: null,
    files: null,
    events: null,
    placement: null,
    selected: null,
    before: null,        // the selected file's copies just before the last repair: { fileId, copies: Set }
    repairing: false,
    uploading: false
  };

  // ------------------------------------------------------------------
  // Small DOM helpers: text only
  // ------------------------------------------------------------------

  function byId(id) {
    return document.getElementById(id);
  }

  function setAttr(node, name, value) {
    if (ATTRIBUTES.indexOf(name) < 0 && name.indexOf('data-') !== 0) {
      throw new Error('attribute not allowed: ' + name);
    }
    node.setAttribute(name, String(value));
  }

  // el('span', { cls: 'a b', text: 'hello', attrs: { title: 'x' } }, [children])
  function el(tag, options, children) {
    var node = document.createElement(tag);
    options = options || {};
    if (options.cls) {
      node.className = options.cls;
    }
    if (options.text !== undefined && options.text !== null) {
      node.textContent = String(options.text);
    }
    if (options.attrs) {
      Object.keys(options.attrs).forEach(function (name) {
        setAttr(node, name, options.attrs[name]);
      });
    }
    (children || []).forEach(function (child) {
      if (child) {
        node.appendChild(child);
      }
    });
    return node;
  }

  function svgIcon(paths) {
    var ns = 'http://www.w3.org/2000/svg';
    var svg = document.createElementNS(ns, 'svg');
    [['viewBox', '0 0 24 24'], ['width', '22'], ['height', '22'], ['fill', 'none'], ['stroke', 'currentColor'],
      ['stroke-width', '2'], ['stroke-linecap', 'round'], ['stroke-linejoin', 'round'], ['aria-hidden', 'true']]
      .forEach(function (pair) { setAttr(svg, pair[0], pair[1]); });
    paths.forEach(function (d) {
      var path = document.createElementNS(ns, 'path');
      setAttr(path, 'd', d);
      svg.appendChild(path);
    });
    return svg;
  }

  function clear(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function replaceChildren(node, children) {
    clear(node);
    children.forEach(function (child) { node.appendChild(child); });
  }

  // ------------------------------------------------------------------
  // Formatting
  // ------------------------------------------------------------------

  // A stored file name as shown on the page: exactly as uploaded, except that control characters
  // (which a file on macOS or Linux may have) are shown as visible symbols instead of acting.
  function displayName(name) {
    var out = '';
    for (var i = 0; i < name.length; i++) {
      var c = name.charCodeAt(i);
      out += c < 0x20 ? String.fromCharCode(0x2400 + c) : c === 0x7f ? String.fromCharCode(0x2421)
        : c >= 0x80 && c <= 0x9f ? String.fromCharCode(0xfffd) : name.charAt(i);
    }
    return out;
  }

  // What a file is called on the page: its name, or its short ID if it was stored without one.
  function fileLabel(fileId, name) {
    return typeof name === 'string' ? displayName(name) : shortId(fileId);
  }

  function shortId(id) {
    return id.length > 14 ? id.slice(0, 8) + '…' + id.slice(-4) : id;
  }

  function formatBytes(bytes) {
    var units = ['bytes', 'KiB', 'MiB', 'GiB'];
    var value = bytes;
    var unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit++;
    }
    if (unit === 0) {
      return bytes + (bytes === 1 ? ' byte' : ' bytes');
    }
    var text = value >= 10 || Math.round(value) === value ? String(Math.round(value)) : value.toFixed(1);
    return text + ' ' + units[unit];
  }

  function plural(count, one, many) {
    return count + ' ' + (count === 1 ? one : many);
  }

  function clockTime(iso) {
    var date = new Date(iso);
    if (isNaN(date.getTime())) {
      return '';
    }
    function two(n) { return (n < 10 ? '0' : '') + n; }
    return two(date.getHours()) + ':' + two(date.getMinutes()) + ':' + two(date.getSeconds());
  }

  function ago(iso) {
    var millis = Date.now() - new Date(iso).getTime();
    if (isNaN(millis)) {
      return 'never';
    }
    var seconds = Math.max(0, millis / 1000);
    if (seconds < 10) {
      return seconds.toFixed(1) + ' s ago';
    }
    if (seconds < 90) {
      return Math.round(seconds) + ' s ago';
    }
    return clockTime(iso);
  }

  function listNodes(indexes) {
    if (indexes.length === 1) {
      return 'Node ' + indexes[0];
    }
    return 'Nodes ' + indexes.slice(0, -1).join(', ') + ' and ' + indexes[indexes.length - 1];
  }

  // ------------------------------------------------------------------
  // Requests: same origin only
  // ------------------------------------------------------------------

  function HttpError(status, code) {
    this.status = status;
    this.code = code;
  }

  function request(method, url) {
    return fetch(url, { method: method, cache: 'no-store', credentials: 'same-origin', redirect: 'error',
      headers: { 'Accept': 'application/json' } })
      .then(function (response) {
        return response.json().catch(function () { return null; }).then(function (body) {
          if (!response.ok) {
            throw new HttpError(response.status, body && typeof body.error === 'string' ? body.error : '');
          }
          return body;
        });
      });
  }

  function getJson(url) {
    return request('GET', url);
  }

  function serverReached(ok) {
    if (state.reachable !== ok) {
      state.reachable = ok;
      renderHealth();
    }
  }

  // ------------------------------------------------------------------
  // Polling
  // ------------------------------------------------------------------

  function poller(interval, work) {
    var timer = null;
    var running = false;
    var again = false;

    function schedule(ms) {
      if (timer === null && !document.hidden) {
        timer = setTimeout(tick, ms);
      }
    }

    function tick() {
      timer = null;
      if (document.hidden) {
        return;
      }
      running = true;
      work().then(function () {
        serverReached(true);
        return interval;
      }, function (error) {
        if (error instanceof HttpError) {
          serverReached(true);
          return interval;
        }
        serverReached(false);
        return RETRY_MS;
      }).then(function (next) {
        running = false;
        if (again) {
          again = false;
          schedule(0);
        } else {
          schedule(next);
        }
      });
    }

    return {
      now: function () {
        if (running) {
          again = true;
          return;
        }
        if (timer !== null) {
          clearTimeout(timer);
          timer = null;
        }
        schedule(0);
      },
      stop: function () {
        if (timer !== null) {
          clearTimeout(timer);
          timer = null;
        }
      }
    };
  }

  var statusPoller = poller(STATUS_MS, function () {
    return getJson('/cluster/status').then(function (status) {
      state.status = status;
      renderHealth();
      renderNodes();
    });
  });

  var dataPoller = poller(DATA_MS, function () {
    return Promise.all([getJson('/files'), getJson('/cluster/events')]).then(function (results) {
      state.files = results[0];
      state.events = results[1];
      var ids = state.files.files.map(function (f) { return f.fileId; });
      if (state.selected === null || ids.indexOf(state.selected) < 0) {
        state.selected = ids.length > 0 ? ids[0] : null;
      }
      renderFiles();
      renderActivity();
      renderHealth();
      renderNodes();
      if (state.selected === null) {
        state.placement = null;
        renderMap();
        return null;
      }
      var wanted = state.selected;
      return getJson('/files/' + encodeURIComponent(wanted) + '/placement').then(function (placement) {
        if (state.selected === wanted) {
          state.placement = placement;
          renderMap();
        }
      }, function (error) {
        if (error instanceof HttpError && error.status === 404) {
          state.placement = null;
          renderMap();
          return null;
        }
        throw error;
      });
    }).then(function () {
      setAttr(document.body, 'data-console-ready', 'true');
    });
  });

  document.addEventListener('visibilitychange', function () {
    if (document.hidden) {
      statusPoller.stop();
      dataPoller.stop();
    } else {
      statusPoller.now();
      dataPoller.now();
    }
  });

  // ------------------------------------------------------------------
  // Top bar
  // ------------------------------------------------------------------

  function renderHealth() {
    var title = byId('health-title');
    var reason = byId('health-reason');
    var dot = byId('health-dot');
    if (state.reachable === false) {
      title.textContent = 'Server not reachable';
      reason.textContent = 'Trying again every ' + (RETRY_MS / 1000) + ' s';
      dot.className = 'health-dot is-bad';
      return;
    }
    if (!state.status) {
      return;
    }
    var status = state.status;
    var replicas = status.replicationFactor;
    byId('cluster-facts').textContent = plural(replicas, 'copy', 'copies') + ' per chunk · heartbeat every '
      + (status.heartbeatIntervalMillis / 1000) + ' s';
    byId('drop-hint').textContent = 'Split into chunks, ' + plural(replicas, 'copy', 'copies') + ' each';

    var down = status.nodes.filter(function (n) { return n.state !== 'UP'; }).map(function (n) { return n.index; });
    var parts = [down.length === 0 ? 'All ' + status.nodes.length + ' nodes up'
      : listNodes(down) + (down.length === 1 ? ' is down' : ' are down')];
    var degraded = down.length > 0;
    if (state.files) {
      var files = state.files.files;
      var count = function (health) { return files.filter(function (f) { return f.health === health; }).length; };
      var missing = count('missing');
      var unavailable = count('unavailable');
      var atRisk = count('at_risk');
      var extra = count('over_replicated');
      if (files.length === 0) {
        parts.push('no files yet');
      } else if (missing + unavailable + atRisk > 0) {
        degraded = true;
        if (missing > 0) {
          parts.push(plural(missing, 'file has', 'files have') + ' a chunk with no copy');
        }
        if (unavailable > 0) {
          parts.push(plural(unavailable, 'file is', 'files are') + ' unavailable');
        }
        if (atRisk > 0) {
          parts.push(plural(atRisk, 'file has', 'files have') + ' chunks with too few copies');
        }
      } else if (extra > 0) {
        parts.push('every chunk has at least ' + plural(replicas, 'copy', 'copies'));
      } else {
        parts.push('every chunk has ' + plural(replicas, 'copy', 'copies'));
      }
    }
    title.textContent = degraded ? 'Degraded' : 'Healthy';
    reason.textContent = parts.join(' · ');
    dot.className = 'health-dot ' + (degraded ? 'is-bad' : 'is-ok');
  }

  // ------------------------------------------------------------------
  // Node cards
  // ------------------------------------------------------------------

  function stat(label, value, small) {
    return el('div', { cls: 'stat' }, [
      el('dt', { text: label }),
      el('dd', { cls: small ? 'is-small' : '', text: value })
    ]);
  }

  function renderNodes() {
    if (!state.status) {
      return;
    }
    var listings = state.files ? state.files.nodes : [];
    var cards = state.status.nodes.map(function (node) {
      var up = node.state === 'UP';
      var listing = listings[node.index];
      var copies = listing && listing.listed ? String(listing.chunkCopies) : '–';
      var lastGood = node.lastGoodCheck === null ? 'never' : (up ? ago(node.lastGoodCheck) : clockTime(node.lastGoodCheck));
      return el('article', { cls: 'node-card glass ' + (up ? 'is-up' : 'is-down'),
        attrs: { 'aria-label': 'Node ' + node.index + ', ' + node.state } }, [
        el('div', { cls: 'node-head' }, [
          el('span', { cls: 'node-index', text: node.index, attrs: { 'aria-hidden': 'true' } }),
          el('div', {}, [
            el('div', { cls: 'node-name', text: 'Node ' + node.index }),
            el('div', { cls: 'node-address mono', text: node.address })
          ]),
          el('span', { cls: 'pill ' + (up ? 'is-up' : 'is-down'), text: node.state })
        ]),
        el('div', { cls: 'node-id mono', text: node.nodeId === null ? 'node ID not known yet' : node.nodeId }),
        el('dl', { cls: 'stats' }, [
          stat('Copies', copies),
          stat('Misses', node.failuresInARow),
          stat('Last good', lastGood, true)
        ])
      ]);
    });
    replaceChildren(byId('node-cards'), cards);
  }

  // ------------------------------------------------------------------
  // Chunk map
  // ------------------------------------------------------------------

  function tile(kind, label) {
    var text = { copy: 'copy', 'new': 'new', unreachable: 'unreachable', extra: 'extra', empty: '·', unknown: '?' }[kind];
    return el('span', { cls: 'tile tile-' + kind, attrs: { title: label } }, [
      el('span', { text: text, attrs: { 'aria-hidden': 'true' } }),
      el('span', { cls: 'visually-hidden', text: label })
    ]);
  }

  function columnStatus(chunk, replicas) {
    var reachable = chunk.reachableCopies;
    if (reachable > replicas) {
      return { text: reachable + ' copies · extra', cls: 'is-extra' };
    }
    if (reachable < replicas) {
      return { text: reachable + ' of ' + replicas + ' · ' + (reachable === 0 ? 'none reachable' : 'at risk'),
        cls: 'is-risk' };
    }
    return { text: reachable + ' of ' + replicas, cls: '' };
  }

  function renderMap() {
    var scroll = byId('map-scroll');
    var more = byId('map-more');
    var placement = state.placement;
    if (!placement) {
      byId('map-subtitle').textContent = state.files && state.files.files.length > 0 ? 'Loading the selected file'
        : 'No file yet: upload one to see where its chunks are';
      replaceChildren(scroll, []);
      more.hidden = true;
      return;
    }
    var replicas = placement.replicationFactor;
    byId('map-subtitle').textContent = fileLabel(placement.fileId, placement.name) + ' · ' + formatBytes(placement.size)
      + ' · ' + plural(placement.chunkCount, 'chunk', 'chunks') + ' of ' + formatBytes(placement.chunkSize)
      + ' · each row is a node';
    var shown = placement.chunks.slice(0, MAX_COLUMNS);
    var before = state.before && state.before.fileId === placement.fileId ? state.before.copies : null;

    var headRow = el('tr', {}, [el('td', {})]);
    shown.forEach(function (chunk) {
      var status = columnStatus(chunk, replicas);
      headRow.appendChild(el('th', { attrs: { scope: 'col' } }, [
        el('span', { text: 'chunk ' + chunk.index }),
        el('span', { cls: 'col-status ' + status.cls, text: status.text })
      ]));
    });

    var rows = placement.nodes.map(function (node) {
      var up = node.state === 'UP';
      var row = el('tr', { cls: up ? '' : 'is-down' }, [
        el('th', { attrs: { scope: 'row' } }, [
          el('span', { cls: 'row-name', text: 'Node ' + node.index }),
          el('span', { cls: 'row-state' + (up ? '' : ' is-down'), text: node.state })
        ])
      ]);
      shown.forEach(function (chunk) {
        var holder = null;
        chunk.holders.forEach(function (h) {
          if (h.node === node.index) {
            holder = h;
          }
        });
        var where = 'chunk ' + chunk.index + ' on node ' + node.index + ': ';
        var cell;
        if (holder === null) {
          cell = node.listed ? tile('empty', where + 'no copy') : tile('unknown', where + 'not known (node never listed)');
        } else if (!holder.reachable) {
          cell = tile('unreachable', where + 'copy, unreachable');
        } else if (holder.trimCandidate) {
          cell = tile('extra', where + 'extra copy; a repair would remove it');
        } else if (before !== null && !before.has(chunk.index + '/' + node.index)) {
          cell = tile('new', where + 'new copy made by the last repair');
        } else {
          cell = tile('copy', where + 'copy');
        }
        row.appendChild(el('td', {}, [cell]));
      });
      return row;
    });

    var table = el('table', { cls: 'map' }, [el('thead', {}, [headRow]), el('tbody', {}, rows)]);
    replaceChildren(scroll, [table]);

    var hidden = placement.chunks.length - shown.length;
    if (hidden > 0) {
      var atRisk = placement.chunks.slice(MAX_COLUMNS).filter(function (c) { return c.reachableCopies < replicas; }).length;
      more.textContent = '+' + plural(hidden, 'more chunk', 'more chunks') + ' not shown'
        + (atRisk > 0 ? ', ' + atRisk + ' of them with too few copies' : ', all with ' + replicas + ' or more copies');
      more.hidden = false;
    } else {
      more.hidden = true;
    }
  }

  function copiesOf(placement) {
    var copies = new Set();
    placement.chunks.forEach(function (chunk) {
      chunk.holders.forEach(function (h) { copies.add(chunk.index + '/' + h.node); });
    });
    return copies;
  }

  // ------------------------------------------------------------------
  // Files and upload
  // ------------------------------------------------------------------

  function badge(file, replicas) {
    switch (file.health) {
      case 'healthy': return { text: plural(replicas, 'copy', 'copies'), cls: '' };
      case 'over_replicated': return { text: 'extra copies', cls: 'is-extra' };
      case 'at_risk': return { text: 'at risk', cls: 'is-risk' };
      case 'unavailable': return { text: 'unavailable', cls: 'is-risk' };
      case 'missing': return { text: 'missing', cls: 'is-risk' };
      default: return { text: String(file.health), cls: 'is-extra' };
    }
  }

  function renderFiles() {
    var data = state.files;
    var rows = data.files.map(function (file) {
      var selected = file.fileId === state.selected;
      var b = badge(file, data.replicationFactor);
      var named = typeof file.name === 'string';
      // The name in a <bdi>, so right-to-left text in it cannot reorder the text around it.
      var select = el('button', { cls: 'file-select', attrs: { type: 'button', 'aria-pressed': selected ? 'true' : 'false' } }, [
        named ? el('bdi', { cls: 'file-name', text: displayName(file.name), attrs: { title: displayName(file.name) } }) : null,
        el('span', { cls: 'file-id mono' + (named ? ' is-secondary' : ''), text: shortId(file.fileId), attrs: { title: file.fileId } }),
        el('span', { cls: 'file-meta', text: formatBytes(file.size) + ' · ' + plural(file.chunkCount, 'chunk', 'chunks') })
      ]);
      select.addEventListener('click', function () {
        if (state.selected !== file.fileId) {
          state.selected = file.fileId;
          state.placement = null;
          renderFiles();
          renderMap();
          dataPoller.now();
        }
      });
      var download = el('a', { cls: 'icon-btn', attrs: { href: '/files/' + encodeURIComponent(file.fileId),
        download: named ? file.name : file.fileId + '.bin' } }, [
        svgIcon(['M12 4v11', 'M7 10l5 5 5-5', 'M5 20h14']),
        el('span', { cls: 'visually-hidden', text: 'Download ' + fileLabel(file.fileId, file.name) })
      ]);
      return el('li', { cls: 'file-row' + (selected ? ' is-selected' : '') }, [
        select,
        el('span', { cls: 'badge ' + b.cls, text: b.text }),
        download
      ]);
    });
    replaceChildren(byId('file-list'), rows);

    var notes = [];
    if (data.truncated) {
      notes.push('Only the newest ' + data.files.length + ' files are listed.');
    }
    if (data.skipped.pending > 0) {
      notes.push(plural(data.skipped.pending, 'upload is', 'uploads are') + ' in progress or interrupted.');
    }
    if (data.skipped.unreadable > 0) {
      notes.push(plural(data.skipped.unreadable, 'manifest', 'manifests') + ' could not be read.');
    }
    var note = byId('files-note');
    note.textContent = notes.join(' ');
    note.hidden = notes.length === 0;
  }

  function uploadStatus(text, isError) {
    var node = byId('upload-status');
    node.textContent = text;
    node.className = 'upload-status' + (isError ? ' is-error' : '');
  }

  function hex(buffer) {
    var bytes = new Uint8Array(buffer);
    var out = '';
    for (var i = 0; i < bytes.length; i++) {
      out += (bytes[i] < 16 ? '0' : '') + bytes[i].toString(16);
    }
    return out;
  }

  function uploadErrorText(status, code) {
    var known = {
      payload_too_large: 'The file is larger than this server accepts.',
      invalid_file_name: 'The server does not accept this file’s name; nothing was stored.',
      checksum_mismatch: 'The server received different bytes than were sent; nothing was stored.',
      storage_error: 'The storage nodes could not store the file; nothing was committed.',
      cross_origin_refused: 'The server refused the request.'
    };
    return known[code] || ('The upload failed (HTTP ' + status + (code ? ', ' + code : '') + ').');
  }

  function upload(file) {
    if (state.uploading || !file) {
      return;
    }
    state.uploading = true;
    byId('choose-file').disabled = true;
    var withHash = window.crypto && window.crypto.subtle && file.size <= HASH_LIMIT_BYTES;
    uploadStatus(withHash ? 'Computing the SHA-256 of ' + formatBytes(file.size) + '…' : 'Uploading…', false);
    file.arrayBuffer().then(function (buffer) {
      return (withHash ? window.crypto.subtle.digest('SHA-256', buffer).then(hex) : Promise.resolve(null))
        .then(function (sha) { return { buffer: buffer, sha: sha }; });
    }).then(function (prepared) {
      return new Promise(function (resolve, reject) {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/files');
        xhr.setRequestHeader('Content-Type', 'application/octet-stream');
        if (prepared.sha) {
          xhr.setRequestHeader('X-Content-SHA256', prepared.sha);
        }
        // The original name, so the file downloads under it again. encodeURIComponent writes
        // percent-encoded UTF-8; it throws only for a name that is not valid Unicode.
        var encodedName = null;
        try {
          encodedName = encodeURIComponent(file.name);
        } catch (ignored) {
          encodedName = null;
        }
        if (encodedName) {
          xhr.setRequestHeader('X-File-Name', encodedName);
        }
        xhr.upload.addEventListener('progress', function (event) {
          if (event.lengthComputable) {
            uploadStatus('Uploading… ' + Math.floor(event.loaded * 100 / event.total) + ' %', false);
          }
        });
        xhr.addEventListener('load', function () {
          var body = null;
          try {
            body = JSON.parse(xhr.responseText);
          } catch (ignored) {
            body = null;
          }
          if (xhr.status === 201 && body && typeof body.fileId === 'string') {
            resolve(body);
          } else {
            reject(new Error(uploadErrorText(xhr.status, body && typeof body.error === 'string' ? body.error : '')));
          }
        });
        xhr.addEventListener('error', function () { reject(new Error('The server could not be reached.')); });
        xhr.send(prepared.buffer);
      });
    }).then(function (manifest) {
      uploadStatus('Stored ' + fileLabel(manifest.fileId, manifest.name) + ': ' + plural(manifest.chunkCount, 'chunk', 'chunks')
        + (manifest.sha256 ? ', SHA-256 checked by the server' : ''), false);
      state.selected = manifest.fileId;
      state.placement = null;
      dataPoller.now();
    }, function (error) {
      uploadStatus(error && error.message ? error.message : 'The upload failed.', true);
    }).then(function () {
      state.uploading = false;
      byId('choose-file').disabled = false;
      byId('file-input').value = '';
    });
  }

  function setUpUpload() {
    var zone = byId('dropzone');
    var input = byId('file-input');
    byId('choose-file').addEventListener('click', function () { input.click(); });
    input.addEventListener('change', function () {
      if (input.files && input.files.length > 0) {
        upload(input.files[0]);
      }
    });
    ['dragenter', 'dragover'].forEach(function (type) {
      zone.addEventListener(type, function (event) {
        event.preventDefault();
        zone.classList.add('is-over');
      });
    });
    ['dragleave', 'drop'].forEach(function (type) {
      zone.addEventListener(type, function (event) {
        event.preventDefault();
        zone.classList.remove('is-over');
      });
    });
    zone.addEventListener('drop', function (event) {
      var files = event.dataTransfer && event.dataTransfer.files;
      if (files && files.length > 0) {
        upload(files[0]);
      }
    });
  }

  // ------------------------------------------------------------------
  // Activity feed
  // ------------------------------------------------------------------

  function eventText(e) {
    switch (e.type) {
      case 'node_down':
        return { text: 'Node ' + e.node + ' DOWN after ' + plural(e.failuresInARow, 'missed heartbeat', 'missed heartbeats'),
          cls: 'is-bad' };
      case 'node_up':
        return { text: 'Node ' + e.node + ' UP again', cls: 'is-ok' };
      case 'upload_finished':
        return { text: 'Upload ' + fileLabel(e.fileId, e.name) + ' finished · ' + plural(e.chunks, 'chunk', 'chunks')
          + ' · ' + plural(e.copies, 'copy', 'copies'), cls: 'is-ok' };
      case 'repair_finished':
        return { text: repairSummary(e) + ' · ' + plural(e.chunksChecked, 'chunk', 'chunks') + ' checked',
          cls: e.chunksLost > 0 ? 'is-bad' : 'is-ok' };
      default:
        return { text: String(e.type), cls: '' };
    }
  }

  function repairSummary(r) {
    var verb = r.dryRun ? 'Dry run: would ' : 'Repair ';
    var parts = [];
    if (r.copiesAdded > 0) {
      parts.push((r.dryRun ? 'add ' : 'added ') + plural(r.copiesAdded, 'copy', 'copies'));
    }
    if (r.copiesReplaced > 0) {
      parts.push((r.dryRun ? 'replace ' : 'replaced ') + plural(r.copiesReplaced, 'damaged copy', 'damaged copies'));
    }
    if (r.copiesRemoved > 0) {
      parts.push((r.dryRun ? 'remove ' : 'removed ') + plural(r.copiesRemoved, 'copy', 'copies'));
    }
    if (parts.length === 0) {
      return r.dryRun ? 'Dry run: nothing to repair' : 'Repair: nothing to do';
    }
    return verb + parts.join(', ');
  }

  function renderActivity() {
    var events = state.events.events.slice(0, MAX_EVENTS_SHOWN);
    var items = events.map(function (e) {
      var t = eventText(e);
      return el('li', { cls: 'event' }, [
        el('span', { cls: 'event-dot ' + t.cls, attrs: { 'aria-hidden': 'true' } }),
        el('span', { text: t.text }),
        el('time', { cls: 'event-time mono', text: clockTime(e.time), attrs: { datetime: e.time } })
      ]);
    });
    replaceChildren(byId('activity'), items);
    byId('activity-empty').hidden = items.length > 0;
  }

  // ------------------------------------------------------------------
  // Repair and the report sheet
  // ------------------------------------------------------------------

  var lastFocus = null;

  function openSheet(kicker, title, stats, notes) {
    byId('sheet-kicker').textContent = kicker;
    byId('sheet-title').textContent = title;
    replaceChildren(byId('sheet-stats'), stats.map(function (s) { return stat(s[0], s[1]); }));
    replaceChildren(byId('sheet-notes'), notes.map(function (n) { return el('li', { cls: n[1] || '', text: n[0] }); }));
    lastFocus = document.activeElement;
    byId('sheet-backdrop').hidden = false;
    byId('sheet').hidden = false;
    byId('sheet-close').focus();
  }

  function closeSheet() {
    if (byId('sheet').hidden) {
      return;
    }
    byId('sheet').hidden = true;
    byId('sheet-backdrop').hidden = true;
    if (lastFocus && typeof lastFocus.focus === 'function') {
      lastFocus.focus();
    }
  }

  function trapFocus(event) {
    var sheet = byId('sheet');
    if (sheet.hidden) {
      return;
    }
    if (event.key === 'Escape') {
      event.preventDefault();
      closeSheet();
      return;
    }
    if (event.key !== 'Tab') {
      return;
    }
    var focusable = Array.prototype.slice.call(sheet.querySelectorAll('button, a[href]'));
    if (focusable.length === 0) {
      return;
    }
    var first = focusable[0];
    var last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    } else if (!sheet.contains(document.activeElement)) {
      event.preventDefault();
      first.focus();
    }
  }

  function showReport(r) {
    var title = repairSummary(r).replace(/^Repair /, '').replace(/^Dry run: /, '');
    title = title.charAt(0).toUpperCase() + title.slice(1);
    var stats = [
      ['Chunks checked', r.chunksChecked],
      [r.dryRun ? 'Copies to add' : 'Copies added', r.copiesAdded],
      [r.dryRun ? 'Copies to replace' : 'Copies replaced', r.copiesReplaced],
      [r.dryRun ? 'Copies to remove' : 'Copies removed', r.copiesRemoved],
      ['Chunks lost', r.chunksLost],
      ['Duration', (r.durationMillis / 1000).toFixed(2) + ' s']
    ];
    if (r.chunksUncertain > 0) {
      stats.push(['Chunks uncertain', r.chunksUncertain]);
    }
    if (r.chunksNotRepaired > 0) {
      stats.push(['Chunks not repaired', r.chunksNotRepaired]);
    }
    var notes = [];
    notes.push([r.dryRun ? 'A dry run reads and checks the same copies as a repair, and changes nothing.'
      : 'Every new copy was read back and its SHA-256 checked against the manifest before it counted.']);
    if (r.trimSkipped) {
      notes.push(['Extra copies were kept, because a node was DOWN or failed during the repair.']);
    }
    if (r.filesSkipped > 0) {
      notes.push([plural(r.filesSkipped, 'file was', 'files were') + ' skipped: an upload in progress, an unreadable'
        + ' manifest, or a file stored before per-chunk checksums.']);
    }
    if (r.nodesFailedDuringRepair.length > 0) {
      notes.push([listNodes(r.nodesFailedDuringRepair) + ' failed during the repair and ' +
        (r.nodesFailedDuringRepair.length === 1 ? 'was' : 'were') + ' not used again.', 'is-bad']);
    }
    if (r.chunksLost > 0) {
      notes.push(['Lost chunks (no good copy on any node): ' + r.lost.join(', '), 'is-bad']);
    }
    openSheet(r.dryRun ? 'Dry run finished' : 'Repair finished', title, stats, notes);
  }

  function runRepair(dryRun) {
    if (state.repairing) {
      return;
    }
    state.repairing = true;
    var dry = byId('dry-run');
    var repair = byId('repair');
    dry.disabled = true;
    repair.disabled = true;
    (dryRun ? dry : repair).textContent = dryRun ? 'Checking…' : 'Repairing…';
    if (!dryRun && state.placement) {
      state.before = { fileId: state.placement.fileId, copies: copiesOf(state.placement) };
    }
    request('POST', '/cluster/repair' + (dryRun ? '?dryRun=true' : '')).then(showReport, function (error) {
      var text = error instanceof HttpError && error.code === 'repair_in_progress'
        ? 'A repair is already running; try again when it has finished.'
        : error instanceof HttpError ? 'The repair failed (HTTP ' + error.status + (error.code ? ', ' + error.code : '') + ').'
          : 'The server could not be reached.';
      openSheet(dryRun ? 'Dry run' : 'Repair', 'Not run', [], [[text, 'is-bad']]);
    }).then(function () {
      state.repairing = false;
      dry.disabled = false;
      repair.disabled = false;
      dry.textContent = 'Dry run';
      repair.textContent = 'Repair';
      statusPoller.now();
      dataPoller.now();
    });
  }

  // ------------------------------------------------------------------
  // Start
  // ------------------------------------------------------------------

  function countOn(name) {
    var body = document.body;
    setAttr(body, name, Number(body.getAttribute(name) || '0') + 1);
  }

  // For scripts\console-smoke.ps1, which reads these from the page: errors and CSP violations.
  window.addEventListener('error', function () { countOn('data-js-errors'); });
  window.addEventListener('unhandledrejection', function () { countOn('data-js-errors'); });
  document.addEventListener('securitypolicyviolation', function () { countOn('data-csp-violations'); });

  function start() {
    byId('server-address').textContent = window.location.host;
    byId('dry-run').addEventListener('click', function () { runRepair(true); });
    byId('repair').addEventListener('click', function () { runRepair(false); });
    byId('sheet-close').addEventListener('click', closeSheet);
    byId('sheet-backdrop').addEventListener('click', closeSheet);
    document.addEventListener('keydown', trapFocus);
    setUpUpload();
    statusPoller.now();
    dataPoller.now();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
}());
