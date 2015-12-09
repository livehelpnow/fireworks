# Changelog

## v0.5.1
  * Allow global json options to be defined.

## v0.5.0
  * Remove direct references to Poison. json_library / json_opts should be passed in config
    If these options are passed, it will decide / encode using the json lib and options
    Otherwise, the message will be transmitted as a plain binary

## v0.4.0
  * Added `Fireworks.Logger` backend for rabbit logging.

## v0.3.4
  * Handle :normal shutdown for Task

## v0.3.3
  * Add poolboy to application list

## v0.3.2
  * Channel state was not being set on EXIT

## v0.3.1
  * Fixed issue with spec return types

## v0.3.0
  * Pooled connections

## v0.2.0
  * Changed underlying connection and registration scheme
  * Stability!

## v0.1.0
  * Initial Release
