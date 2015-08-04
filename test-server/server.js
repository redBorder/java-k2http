'use strict';

var http = require('http');
var winston = require('winston');
var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');

var logger = new(winston.Logger)({
    transports: [new(winston.transports.Console)({
        level: 'debug',
        prettyPrint: true,
        colorize: true,
        timestamp: true
    })]
});

var app = express();

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
    logger.debug(info);
    res.end();
});

app.listen(port);
logger.info('Servidor iniciado en: ' + port + '/TCP');
