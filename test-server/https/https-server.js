'use strict';

var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var winston = require('winston');
var fs = require('fs');
var https = require('https');
var _ = require('lodash');

var logger = new(winston.Logger)({
    transports: [new(winston.transports.Console)({
        level: 'debug',
        prettyPrint: true,
        colorize: true,
        timestamp: true
    })]
});

var privateKey = fs.readFileSync('localhost.key', 'utf8');
var certificate = fs.readFileSync('localhost.crt', 'utf8');

var httpsServer = https.createServer({
    key: privateKey,
    cert: certificate,
    passphrase: 'arcadio'
}, app);


    app.use(bodyParser.urlencoded({
        extended: true
    }));
    app.use(bodyParser.json());

    var port = process.env.PORT || 8080;


app.post('/:uuid/:topic', function (req, res) {
    var info = {};
    _.assign(info, req.body);
    info.endpoint = req.params.topic;
    info.uuid = req.params.uuid;
    logger.debug(info.endpoint);
    logger.debug(info.uuid);
    res.end();
});

    httpsServer.listen(port);
    logger.info('Servidor iniciado en: ' + port + '/TCP');



