/**
 *
 * Author: Lukas Reichart on 3/9/15.
 * Purpose:
 * Copyright @Antum
 */

var Writable = require('stream').Writable;
var Transform = require('stream').Transform;
var azure = require( 'azure-storage');
var _ = require( 'lodash' );
var mime = require( 'mime' );

module.exports = function SkipperAzure( globalOptions ) {
  globalOptions = globalOptions || {};
  var blobService = azure.createBlobService( globalOptions.key,
    globalOptions.secret );

  var adapter = {

    read: function( fd, cb ) {
      console.log( "Reading" );
      var prefix = fd;

      // Build a noop transform stream that will pump the S3 output through
      var __transform__ = new Transform();
      __transform__._transform = function (chunk, encoding, callback) {
        return callback(null, chunk);
      };

      blobService.getBlobToStream( options.container, __transform__, function( error, result, response ) {
        if ( error ) {
          callback( error );
        }
      });
    },

    rm: function( fd, cb ) {
      console.log( "Removing" );
      cb( null );
      //blobService.deleteBlobIfExists( options.container, fd, function( err, result ) {
      //  cb( err, result );
      //});
    },

    ls: function( fd, cb ) {
      console.log( "ls" );
      //blobService.listBlobsSegmentedWithPrefix()
      cb(null);
    },

    receive: AzureReceiver
  };

  return adapter;


  /**
   * A simple receiver for Skipper that writes Upstreams to Azure Blob Storage
   * to the configured container.
   *
   * @param {Object} options
   * @returns {Stream.Writable}
   */
  function AzureReceiver( options ) {

    options = options || {};
    options = _.defaults( options, globalOptions );

    var receiver = Writable({
      objectMode: true
    });

    receiver.once( 'error', function( err ) {
      console.log( 'ERROR ON RECEIVER :: ', err );
    });

    receiver._write = function onFile( newFile, encoding, done ) {


      var startedAt = new Date();
      console.log( "Writing" );
      return done();

      newFile.once( 'error', function( err ) {
        console.log( ('ERROR OIN file read stream in receiver (%s) :: ', newFile.filename, err ).red );
      });

      var headers = options.headers || {};

      // Lookup content type with mime if not set
      if ( typeof headers['content-type'] === 'undefined' ) {
        headers['content-type'] = mime.lookup( newFile.fd );
      }

      var uploadOptions = {
        contentType: headers['content-type']
      };

      var uploader = blobService.createBlockBlobFromStream( options.container,
        newFile.fd, newFile, newFile._readableState.length, uploadOptions, function( err, result, response ) {
          if( err ) {
            console.log( ('Receiver: Error writing ' + newFile.filename + ' :: Cancelling upload and cleaning up already-written bytes ... ' ).red );
            receiver.emit( 'error', err );
            return;
          }

          newFile.extra = response;

          var endedAt = new Date();
          var duration = ( endedAt - startedAt ) / 1000;

          console.log( 'UPLOAD took ' + duration + ' seconds .. ' );

          done();
        });
    };
    return receiver;
  };

};
