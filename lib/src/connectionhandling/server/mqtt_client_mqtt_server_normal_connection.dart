/*
 * Package : mqtt_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 22/06/2017
 * Copyright :  S.Hamblett
 */

part of '../../../mqtt_server_client.dart';

/// The MQTT normal(insecure TCP) server connection class
class MqttServerNormalConnection extends MqttServerConnection<Socket> {
  /// Default constructor
  MqttServerNormalConnection(
    super.eventBus,
    super.socketOptions,
    super.socketTimeout,
  );

  /// Initializes a new instance of the MqttConnection class.
  MqttServerNormalConnection.fromConnect(
    String server,
    int port,
    events.EventBus eventBus,
    List<RawSocketOption> socketOptions,
    Duration socketTimeout,
  ) : super(eventBus, socketOptions, socketTimeout) {
    connect(server, port);
  }

  /// Connect
  @override
  Future<MqttClientConnectionStatus?> connect(String server, int port) {
    final completer = Completer<MqttClientConnectionStatus?>();
    MqttLogger.log('MqttNormalConnection::connect - entered');
    try {
      // Connect and save the socket.
      Socket.connect(server, port, timeout: socketTimeout)
          .then((socket) {
            // Socket options
            final applied = _applySocketOptions(socket, socketOptions);
            if (applied) {
              MqttLogger.log(
                'MqttNormalConnection::connect - socket options applied',
              );
            }
            client = socket;
            readWrapper = ReadWrapper();
            messageStream = MqttByteBuffer(typed.Uint8Buffer());
            _startListening();
            completer.complete();
          })
          .catchError((e) {
            if (_isSocketTimeout(e)) {
              final message =
                  'MqttNormalConnection::connect - The connection to the message broker '
                  '{$server}:{$port} could not be made, a socket timeout has occurred';
              MqttLogger.log(message);
              completer.complete();
            } else {
              onError(e);
              completer.completeError(e);
            }
          })
          .onError((e, s) {
            String message;
            if (e is SocketException) {
              message =
                  'MqttNormalConnection::connect - The connection to the message broker '
                  '{$server}:{$port} is interrupted. Error is ${e.toString()}';
              MqttLogger.log(message);
            }
            message =
                'MqttNormalConnection::connect - The connection to the message broker '
                '{$server}:{$port} could not be made. Error is ${e.toString()}';
            onError(e);
            Error.throwWithStackTrace(NoConnectionException(message), s);
          });
    } on SocketException catch (e, stack) {
      final message =
          'MqttNormalConnection::connect - The connection to the message broker '
          '{$server}:{$port} could not be made. Error is ${e.toString()}';
      completer.completeError(e);
      Error.throwWithStackTrace(NoConnectionException(message), stack);
    } on Exception catch (e, stack) {
      completer.completeError(e);
      final message =
          'MqttNormalConnection::Connect - The connection to the message '
          'broker {$server}:{$port} could not be made.';
      Error.throwWithStackTrace(NoConnectionException(message), stack);
    }
    return completer.future;
  }

  /// Connect Auto
  @override
  Future<MqttClientConnectionStatus?> connectAuto(String server, int port) {
    final completer = Completer<MqttClientConnectionStatus?>();
    MqttLogger.log('MqttNormalConnection::connectAuto - entered');
    try {
      // Connect and save the socket.
      Socket.connect(server, port, timeout: socketTimeout)
          .then((socket) {
            // Socket options
            final applied = _applySocketOptions(socket, socketOptions);
            if (applied) {
              MqttLogger.log(
                'MqttNormalConnection::connectAuto - socket options applied',
              );
            }
            client = socket;
            try {
              _startListening();
              completer.complete();
            } catch (e) {
              onError(e);
              completer.completeError(e);
            }
          })
          .catchError((e) {
            if (_isSocketTimeout(e)) {
              final message =
                  'MqttNormalConnection::connectAuto - The connection to the message broker '
                  '{$server}:{$port} could not be made, a socket timeout has occurred';
              MqttLogger.log(message);
              completer.complete();
            } else {
              onError(e);
              completer.completeError(e);
            }
          })
          .onError((e, s) {
            String message;
            if (e is SocketException) {
              message =
                  'MqttNormalConnection::connect - The connection to the message broker '
                  '{$server}:{$port} is interrupted. Error is ${e.toString()}';
              MqttLogger.log(message);
            }
            message =
                'MqttNormalConnection::connect - The connection to the message broker '
                '{$server}:{$port} could not be made. Error is ${e.toString()}';
            onError(e);
            Error.throwWithStackTrace(NoConnectionException(message), s);
          });
    } on SocketException catch (e, stack) {
      final message =
          'MqttNormalConnection::connectAuto - The connection to the message broker '
          '{$server}:{$port} could not be made. Error is ${e.toString()}';
      completer.completeError(e);
      Error.throwWithStackTrace(NoConnectionException(message), stack);
    } on Exception catch (e, stack) {
      completer.completeError(e);
      final message =
          'MqttNormalConnection::ConnectAuto - The connection to the message '
          'broker {$server}:{$port} could not be made.';
      Error.throwWithStackTrace(NoConnectionException(message), stack);
    }
    return completer.future;
  }

  /// Sends the message in the stream to the broker.
  @override
  void send(MqttByteBuffer message) {
    final messageBytes = message.read(message.length);
    client?.add(messageBytes.toList());
  }

  /// Stops listening the socket immediately.
  @override
  Future<void> stopListening() async {
    final listenersCopy = List<StreamSubscription>.from(listeners);
    listeners.clear(); // Clear first to prevent recursive issues

    await Future.forEach(listenersCopy, (final listener) async {
      try {
        if (!listener.isPaused) {
          await listener.cancel();
        }
      } catch (e) {
        // Listener might already be cancelled or in invalid state
        // This is expected during error conditions, don't propagate
        MqttLogger.log('MqttServerNormalConnection::stopListening - error cancelling listener: $e');
      }
    });
  }

  /// Closes the socket immediately.
  @override
  Future<void> closeClient() async {
    if (client != null) {
      try {
        client!.destroy();
      } catch (e) {
        MqttLogger.log('MqttServerNormalConnection::closeClient - error in destroy(): $e');
        // Don't rethrow, socket might already be closed
      }
      try {
        await client!.close();
      } catch (e) {
        MqttLogger.log('MqttServerNormalConnection::closeClient - error in close(): $e');
        // Don't rethrow, socket might already be closed
      }
    }
  }

  /// Closes and dispose the socket immediately.
  @override
  Future<void> disposeClient() async {
    await closeClient();
    if (client != null) {
      client = null;
    }
  }

  /// Implement stream subscription
  @override
  StreamSubscription onListen() {
    final socket = client;
    if (socket == null) {
      throw StateError('socket is null');
    }

    return socket.listen(onData, onError: onError, onDone: onDone);
  }
}
