/*
 * Package : mqtt_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 29/03/2020
 * Copyright :  S.Hamblett
 */

part of '../../mqtt_client.dart';

/// The MQTT client connection base class
abstract class MqttConnectionBase<T extends Object> {
  /// The socket that maintains the connection to the MQTT broker.
  @protected
  T? client;

  /// The stream controller as returned when clients listen.
  @protected
  List<StreamSubscription> listeners = [];

  /// The read wrapper
  @protected
  ReadWrapper? readWrapper;

  ///The read buffer
  @protected
  late MqttByteBuffer messageStream;

  /// Unsolicited disconnection callback
  @internal
  DisconnectCallback? onDisconnected;

  /// The event bus
  @protected
  events.EventBus? clientEventBus;

  /// Default constructor
  MqttConnectionBase(this.clientEventBus);

  /// Initializes a new instance of the MqttConnection class.
  MqttConnectionBase.fromConnect(String server, int port, this.clientEventBus) {
    connect(server, port);
  }

  /// Connect, must be overridden in connection classes
  Future<void> connect(String server, int port);

  /// Connect for auto reconnect , must be overridden in connection classes
  Future<void> connectAuto(String server, int port);

  /// Sends the message in the stream to the broker.
  void send(MqttByteBuffer message);

  /// Stops listening the socket immediately, must be overridden in connection classes
  Future<void> stopListening();

  /// Closes the socket immediately, must be overridden in connection classes
  Future<void> closeClient();

  /// Closes and dispose the socket immediately, must be overridden in connection classes
  @mustCallSuper
  Future<void> disposeClient();

  /// OnError listener callback
  @protected
  Future<void> onError(Object? error, [StackTrace? stackTrace]) async {
    await _disconnect();
    if (onDisconnected != null) {
      MqttLogger.log(
        'MqttConnectionBase::_onError - calling disconnected callback',
      );
      onDisconnected!();
    }
  }

  /// OnDone listener callback
  @protected
  Future<void> onDone() async {
    await _disconnect();
    if (onDisconnected != null) {
      MqttLogger.log(
        'MqttConnectionBase::_onDone - calling disconnected callback',
      );
      onDisconnected!();
    }
  }

  /// User requested or auto disconnect disconnection
  @protected
  Future<void> disconnect({bool auto = false}) async {
    if (auto) {
      await _disconnect();
    } else {
      await onDone();
    }
  }

  /// Internal disconnect with stop listeners and dispose client
  Future<void> _disconnect() async {
    try {
      await stopListening();
    } catch (e) {
      MqttLogger.log('MqttConnectionBase::_disconnect - error in stopListening(): $e');
      rethrow;
    }
    try {
      await disposeClient();
    } catch (e) {
      MqttLogger.log('MqttConnectionBase::_disconnect - error in disposeClient(): $e');
      rethrow;
    }
  }
}
