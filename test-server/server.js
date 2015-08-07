'use strict';

var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var winston = require('winston');
var fs = require('fs');
var https = require('https');
var http = require('http');

// Instancio el logger
var logger = new (winston.Logger)({
    transports: [new (winston.transports.Console)({
        level: 'debug',
        prettyPrint: true,
        colorize: true,
        timestamp: true
    })]
});

// Puertos de escucha
var httpPort = process.env.HTTP_PORT || 8080;
var httpsPort = process.env.HTTPS_PORT || 4433;

// Certificados
var privateKey = fs.readFileSync('localhost.key', 'utf8');
var certificate = fs.readFileSync('localhost.crt', 'utf8');

// Instacio los servidores HTTP y HTTPS
var httpServer = http.createServer(app);

var httpsServer = https.createServer({
    key: privateKey,
    cert: certificate,
    passphrase: '0000'
}, app);

// Configure el middleware para parseo de JSON
app.use(bodyParser.urlencoded({
    extended: true
}));

app.use(bodyParser.json());

// Escucho mensajes a la API
app.post('/:uuid/:topic', function (req, res) {
    logger.debug('TOPIC: ' + req.params.topic);
    logger.debug('UUID: ' + req.params.uuid);
    logger.debug(req.body);
    res.end();
});

// Inicio la escucha
httpServer.listen(httpPort);
httpsServer.listen(httpsPort);

// Muestro la info
logger.info('Servidor HTTP iniciado en: ' + httpPort + '/TCP');
logger.info('Servidor HTTPS iniciado en: ' + httpsPort + '/TCP');




