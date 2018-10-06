# Laravel Queue Redis Module

This is a Redis module that implements a laravel compatible queue driver for redis with the following features added to
the original PHP-Lua driver:

1. Blocking pop is now reliable.
2. Blocking pop now works on delayed and reserved jobs as well.
3. Timer for delayed and reserved jobs is server side, with milliseconds precision.
4. Laravel queue is now available for other programming languages as well.

## Commands

1. laravel.push <queue-name> <job>
2. laravel.later <queue-name>:delayed <delay-ms> <job>
3. laravel.pop <queue-name> <queue-name>:delayed <queue-name>:reserved <reply-after-ms> <block-for-ms>
4. laravel.delete <queue-name>:reserved <job>
5. laravel.release <queue-name>:delayed <queue-name>:reserved <job> <delay-ms>

## Requirements
1. Redis version 5.0 or higher.
2. cmake > 3.1.

## Build
Running this command will create a `build/liblaravelq.so` file.
`mkdir build && cd build && cmake .. && make`
Place this file in a directory to which your redis-server have access.

## Installation
To load the module to an already running redis server run the following redis command:

    module load <path/to/liblaravelq.so>

To always run redis server with the module, you can add the following to redis.conf configuration file (usually in /etc/redis directory):

    loadmodule </path/to/liblaravelq.so>

After loading modules, laravel.* commands will be available.
