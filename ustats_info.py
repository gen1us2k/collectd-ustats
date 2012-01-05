import collectd,urllib2, json

url = None
old_data = None
VERBOSE_LOGGING = False

def fetch_data(url):
  response = urllib2.urlopen(urllib2.Request(url))
  data = json.loads(response.read())
  return data

def dispatch_value(plugin_instance, info, key, type, type_instance=None):
  if not type_instance:
    type_instance = key

  value = int(info)
  log_verbose('Sending value: %s=%s' % (type_instance, value))

  val = collectd.Values(plugin='ustats_info')
  val.plugin_instance = plugin_instance
  val.type = type
  val.type_instance = type_instance
  val.values = [value]
  val.dispatch()

def configure_callback(conf):
  global url
  for c in conf.children:
    if c.key  == 'UstatsURL':
      url = c.values[0]
    if c.key == 'Verbose':
      VERBOSE_LOGGING = bool(c.values[0])
    else:
      collectd.warning ('ustats_info plugin: Unknown config key: %s.' % c.key)
  log_verbose('Configured with url=%s' % (url))

def getValue(cur_data, old_data):
  if cur_data > old_data and old_data:
    value = cur_data - old_data
  else:
    value = 0
  return value

def read_callback():
  global old_data

  log_verbose('Read callback called')
  data = fetch_data(url)
  if not data:
    collectd.error('ustats plugin: No data received')
    return
  if old_data == None:
    old_data = data

  upstreams = []
  for key in data.keys():
    upstreams.append(key)

  for upstream in upstreams:
    for index in range(len(data[upstream])):
      if data[upstream][index] and data[upstream][index] !=1:
        dispatch_value(upstream, getValue(data[upstream][index][4], old_data[upstream][index][4]), 'http_499_errors',  data[upstream][index][0].partition(":")[0])
        dispatch_value(upstream, getValue(data[upstream][index][5], old_data[upstream][index][5]), 'http_500_errors',  data[upstream][index][0].partition(":")[0])
        dispatch_value(upstream, getValue(data[upstream][index][6], old_data[upstream][index][6]), 'http_503_errors',  data[upstream][index][0].partition(":")[0])
        dispatch_value(upstream, getValue(data[upstream][index][7], old_data[upstream][index][7]), 'tcp_errors',  data[upstream][index][0].partition(":")[0])
        dispatch_value(upstream, getValue(data[upstream][index][13], old_data[upstream][index][13]), 'total_errors', data[upstream][index][0].partition(":")[0])
  old_data = data

def log_verbose(msg):
  if not VERBOSE_LOGGING:
    return
  collectd.info('redis plugin [verbose]: %s' % msg)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)
