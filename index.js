const WebSocket = require('ws');
const Q = require('q');
const got = require('got');
const config = require('config');
const uuidv4 = require('uuid/v4');
const yaml = require('js-yaml');
const fs   = require('fs');

let clients = {};
let service_connections = {};

// These are declared global because they are used by the controllers.
global.wss = new WebSocket.Server({ port: config.port });
global.WebSocketOPEN = WebSocket.OPEN;
global.user_pool_public_keys = null;
global.logger =  null;
global.controllers = {}

// TODO: we should encapsulate user pool and extraction of role into a 
//       different module -- get-auth-user-role -- to allow for multiple authentication sources.
module.exports.start = async function(a_logger) {
  try{
    if(a_logger) logger = a_logger;
    // Load the swagger-ws definitions.
    let swagger = yaml.safeLoad(fs.readFileSync('./api/swagger/swagger-ws.yaml', 'utf8'));
    // Load the controllers.
    if(logger) logger.info('Loading controllers - swagger: ' + JSON.stringify(swagger, null, ' '));
    const cwd = process.cwd();  // Current Working Directory of the main file.
    for(resource in swagger.resources){
      controller = swagger.resources[resource]['x-swagger-router-controller'];
      // The path inside require is always relative to the file containing the call to require 
      // therefore we use an absolute path because this is a module.
      controllers[resource] = require(cwd + '/api/controllers/' + controller);
    }
    if(logger) logger.info('Loaded controllers: ' + JSON.stringify(controllers, null, ' '));
    // Load the public keys.
    user_pool_public_keys = await get_public_keys(config)
    if(logger) logger.debug('user_pool_public_keys' + JSON.stringify(user_pool_public_keys, null, ' '));
    run_server(controllers, swagger);
  }catch(e){
    if(logger) logger.error(e.message);
    process.exit(1);
  }
};

function run_server(controllers, swagger){
  if(logger) logger.info('config: ' + JSON.stringify(config, null, ' '));

  // Connect to all the services. Will try to reconnect if loses connection.
  for(service in config.services){
    if(logger) logger.info('connecting to ' + service);
    openConnection(service);
  }

  // const port = config.port;
  // const wss = new WebSocket.Server({ port: port });

  // Here we send and receive messages to/from clients.
  wss.on('connection', function connection(ws, req) {
    if(logger) logger.info('connection - new connection');
    
    // Initially set the role to Anonymous.  When the client sends the jwt we will set the proper role.
    ws.userRole = 'Anonymous';
    ws.userId = null;
    ws.id = uuidv4();

    // Set the latency.
    ws.connection_ping_tstamp = Math.floor(Date.now());
    ws.ping(noop);
    
    ws.on('message', function incoming(message) {
      if(logger) logger.info('connection on message incoming ' + message + ' - user role: ' + ws.userRole + ' - user id: ' + ws.userId);
      // The message validation happens at the service level.
      // Here we only need to keep track of the transactions so we can respond correctly and route the message to the service.
      messageObj = JSON.parse(message);
      //
      if(messageObj.trans_id && messageObj.service){
        // Save the connection for the transaction id so we can send the reply.
        if(! clients[messageObj.trans_id]){
          if(logger) logger.debug('connection - on message - set clients trans_id ' + messageObj.trans_id);
          clients[messageObj.trans_id] = ws
        }
        // Send to the service or process locally.
        if(logger) logger.info('connection - on message - sending message to ' + messageObj.service);
        if(service_connections[messageObj.service]){
          messageObj.user = {role : ws.userRole, id : ws.userId};
          service_connections[messageObj.service].send(JSON.stringify(messageObj));
        }else{
          if(messageObj.service === '_server_'){
            const resource_def = swagger.resources[messageObj.resource];
            // TODO: Check if user has access.
    
            try{
              // Call the function in the controller.
              var controller = resource_def['x-swagger-router-controller'];
              var operationId = resource_def[messageObj.action].operationId;
              controllers[controller][operationId](messageObj, ws, send_response);
            }catch(e){
              if(logger) logger.error('Bad contoller or operation id for resource ' + messageObj.resource);     
            }
          }else{
            // Unkown service.
            if(logger) logger.error('Could not send message ' + message + ' - ERROR: Unkown service ' + messageObj.service);          
            send_response(messageObj, {code : 500, message : 'Unkown service ' + messageObj.service}, ws);
          }
        }
      }else{
        // Log the error.
        var err = '';
        if(! messageObj.trans_id) err += 'Missing trans_id';
        if(! messageObj.service) err += 'Missing service';
        if(! service_connections[messageObj.service]) err += 'Unknown service ' + messageObj.service;
        if(logger) logger.error('Could not send message ' + message + ' - ERROR: ' + err);
        send_response(messageObj, {code : 500, message : err}, ws);
      }
    });
  
    ws.on('close', async function close() {
      console.log('disconnected');
    });

    // Ping/Pong
    // Keep the connection alive by pinging every 30 seconds (browser typically close websocket after 60 seconds) 
    // and close broken connections. Set the connection latency.
    ws.isAlive = true;
    ws.on('pong', heartbeat);
    function noop() {}
    function heartbeat() {
      ws.connection_latency = (Math.floor(Date.now()) - ws.connection_ping_tstamp) / 2;
      ws.isAlive = true;
      // if(logger) logger.info('latency = ' + ws.connection_latency + ' - role: ' + ws.userRole
      //   + ' - user: ' + ws.userId + ' - id: ' + ws.id);
    }
    const intervalId = setInterval(function ping() {
      // NOTE: the hc_kld-auction_bid-engine service also open a connection to the server 
      //       in order to broadcast messges when event is live.
      wss.clients.forEach(function each(ws) {
        // if (ws.isAlive === false){
        //   console.log('disconnecting', ws.userRole);
        //   return ws.terminate();
        // }
        ws.isAlive = false;
        ws.connection_ping_tstamp = Math.floor(Date.now());
        ws.ping(noop);
        // if(logger) logger.debug('ping')
      });
    }, 30000);
    // Log the latency every 10 minutes.
    setInterval(function () {
      wss.clients.forEach(function each(ws) {
        if(logger) logger.info('latency = ' + ws.connection_latency + ' - role: ' + ws.userRole
          + ' - user: ' + ws.userId + ' - id: ' + ws.id);
      });
    }, 600000);
  });
}

// Here we send and receive messages to/from services.
function openConnection(service){
  const ws = new WebSocket(config.services[service].url);
  ws.onopen = () => {
    if(logger) logger.info('openConnection - WebSocket connection to ' + service + ' established');
    service_connections[service] = ws;
  }
  ws.onclose = () => {
    if(logger) logger.error('openConnection - WebSocket connection to ' + service + ' LOST');
    service_connections[service_connections] = null
    setTimeout(function(){
      openConnection(service);
    }, 1000)
  }
  ws.onerror = (event) => {
    if(logger) logger.error('openConnection to ' + service + ' WebSocket error observed:' + JSON.stringify(event, null, ' '));
  }
  ws.onmessage = function (event) {
    console.log('openConnection onmessage event.data:', event.data);
    const messageObj = JSON.parse(event.data)
    console.log('openConnection onmessage messageObj:', messageObj);
    console.log('openConnection onmessage messageObj trans_id:', messageObj.trans_id);
    if(! clients[messageObj.trans_id]){
      if(logger) logger.error('openConnection onmessage - clients  trans_id' + messageObj.trans_id + ' MISSING');
      return;
    }
    messageObj.service_tstamp = Math.floor(Date.now());
    messageObj.connection_latency = clients[messageObj.trans_id].connection_latency;
    clients[messageObj.trans_id].send(JSON.stringify(messageObj));
    // Remove the connection for this transaction id so that the clients object doesn't grow forever.
    // The entry is only used to forward the message to the correct client when it comes back from the service.
    // Once that is done there is no further use for it.
    delete clients[messageObj.trans_id];
    if(logger) logger.debug('openConnection - onmessage deleted trans_id ' + messageObj.trans_id + ' from clients');
  }
}


function get_public_keys(config){
  var deferred = Q.defer();
	try{
    // Retrieve the user pool public key so we can verify jwt tokens.
    const url = 'https://cognito-idp.' + config.user_pool_region +'.amazonaws.com/' + config.user_pool_id + '/.well-known/jwks.json';
    if(logger) logger.info('Getting user pool public key from ' + url);
    got(url, { json: true }).then(response => {
      const user_pool_public_keys = response.body.keys;
      if(logger) logger.debug('get_public_keys user_pool_public_keys' + JSON.stringify(user_pool_public_keys, null, ' '));
      deferred.resolve(user_pool_public_keys)
    }).catch(error => {
      if(logger) logger.error(error.message);
      deferred.reject(error);
    });
	}catch(e){
		if(logger) logger.error(JSON.stringify(e, null, ' '));
    deferred.reject(e);
	}
	return deferred.promise;
}

const send_response = (messageObj, data, ws) => {
  var res_msg = { 
    trans_id : messageObj.trans_id,
    action : get_response_action_name(messageObj.action, data),
    service : messageObj.service,
    resource : messageObj.resource,
    response : data,
    connection_latency : ws.connection_latency,
  }
  console.log('== send_response:', res_msg)
  try{
    ws.send(JSON.stringify(res_msg));
    // console.log('== send_response ws:', ws)
    console.log('== send_response sent')
  }catch(e){
    if(logger) logger.error('send_response - ' + JSON.stringify(e, null, ' '));
  }
  // console.log('== send_request trans_id:', trans_id)  
}

const get_response_action_name = (action, data) => {
  if(data.code === 200){
    switch(action){
    case 'retrieve':
      return 'RETRIEVED';
    case 'crt_updt':
      return 'CRT_UPDTD';
    case 'delete':
      return 'DELETED';
    case 'patch':
      return 'PATCHED';
    case 'flush':
      return 'FLUSHED';    
    }
  }else{
    switch(action){
    case 'retrieve':
      return 'RETRIEVE_FAIL';
    case 'crt_updt':
      return 'CRT_UPDT_FAIL';
    case 'delete':
      return 'DELETE_FAIL';
    case 'patch':
      return 'PATCH_FAIL';
    case 'flush':
      return 'FLUSH_FAIL';    
    }    
  }
  // It should never get here.
  if(logger) logger.error('get_response_action_name - found unknow action' + action);
  return null;
}
