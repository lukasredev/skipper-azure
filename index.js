/**
 *
 * Author: Lukas Reichart on 3/9/15.
 * Purpose: Skipper adapter ( used by the sails.js framework )
 * License: MIT
 * Copyright Lukas Reichart @Antum 2015
 */

var path = require('path');
var Writable = require('stream').Writable;
var concat = require('concat-stream');
var azure = require( 'azure-storage');
var _ = require( 'lodash' );
var mime = require( 'mime' );

module.exports = function SkipperAzure( globalOptions ) {
  globalOptions = globalOptions || {};

  var blobService = azure.createBlobService( globalOptions.key,
    globalOptions.secret );

  var adapter = {

    read: function( fd, cb ) {
      var prefix = fd;

      var res = blobService.createReadStream( globalOptions.container, prefix, function( err ) {
        if ( err ) {
          cb( err );
        }
      });

      res.pipe(concat(function (data) {
        return cb(null, data);
      }));
    },

    rm: function( fd, cb ) {
      blobService.deleteBlobIfExists( globalOptions.container, fd, function( err, result, response ){
        if( err ) {
          return cb( err );
        }

        // construct response
        cb( null, {
          filename: fd,
          success: true,
          extra: response
        });
      });
    },

    ls: function( dirname, cb ) {
      if ( !dirname ) {
        dirname = '/';
      }

      var prefix = dirname;

      blobService.listBlobsSegmentedWithPrefix( globalOptions.container, prefix,
        null, function( err, result, response ) {
          if( err ) {
            return cb( err );
          }

          var data = _.map( result.entries, 'name');
          data = _.map(data, function snipPathPrefixes (thisPath) {
            thisPath = thisPath.replace(/^.*[\/]([^\/]*)$/, '$1');

            // Join the dirname with the filename
            thisPath = path.join(dirname, path.basename(thisPath));

            return thisPath;
          });
          cb( null, data );
        })
    },

    receive: AzureReceiver
  };

  return adapter;


  /**
   * A simple receiver for Skipper that writes Upstreams to Azure Blob Storage
   * to the configured container at the configured path.
   *
   * @param {Object} options
   * @returns {Stream.Writable}
   */
  function AzureReceiver( options ) {

    options = options || {};
    options = _.defaults( options, globalOptions );
    var bytesRemaining = options.maxBytes;

    var receiver = Writable({
      objectMode: true
    });

    receiver.once( 'error', function( err ) {
      console.log( 'ERROR ON RECEIVER :: ', err );
    });

    receiver._write = function onFile( newFile, encoding, done ) {
      var startedAt = new Date();

      newFile.once( 'error', function( err ) {
        console.log( ('ERROR ON file read stream in receiver (%s) :: ', newFile.filename, err ).red );
      });

      var headers = options.headers || {};

      // Lookup content type with mime if not set
      if ( typeof headers['content-type'] === 'undefined' ) {
        headers['content-type'] = mime.lookup( newFile.fd );
      }

      var uploadOptions = {
        contentType: headers['content-type']
      };

      // TODO: only used for the waterline-adapter-tests, because they do not set the byteCount attribute
      // checkout the issue on: https://github.com/lukasreichart/skipper-azure/pull/2
      if( !newFile.byteCount ){
        newFile.byteCount = newFile._readableState.length;
      }

      var uploader = blobService.createBlockBlobFromStream( options.container,
        newFile.fd, newFile, newFile.byteCount, uploadOptions, function( err, result, response ) {
          if( err ) {
            console.log( ('Receiver: Error writing ' + newFile.filename + ' :: Cancelling upload and cleaning up already-written bytes ... ' ).red );
            receiver.emit( 'error', err );
            return;
          }

            newFile.extra = response;
          newFile.size = new Number( newFile.size );

          var endedAt = new Date();
          var duration = ( endedAt - startedAt ) / 1000;

          //console.log( 'UPLOAD took ' + duration + ' seconds .. ' );

          // TODO ?? is this line necessary: skipper-s3/index.js line: 254 does not use it. But skipper-adapter-tests do not work without this line.
          receiver.emit( 'finish', err, result, response );
          done();
        });
    };
    return receiver;
  }
};
