# CHANGELOG


## v3.68.2 (2025-09-25)

### Bug Fixes

- **history**: Allow list of scans by scan number
  ([`1c80f09`](https://github.com/bec-project/bec/commit/1c80f09cea3bbb9dcf8386cc0df292dc7b2b6b85))


## v3.68.1 (2025-09-25)

### Bug Fixes

- **serializer**: Fix serializer for simple bec types
  ([`ab614fb`](https://github.com/bec-project/bec/commit/ab614fba4d05e75fadf0fb103a905b5611dc6aff))


## v3.68.0 (2025-09-24)

### Features

- **scan_history**: All scan numbers listed in ScanHistory
  ([`e86af1c`](https://github.com/bec-project/bec/commit/e86af1c9eeb1cc6dc864de0e25d97a86c40c6aff))

### Testing

- **scan_history**: Test extended
  ([`f573ace`](https://github.com/bec-project/bec/commit/f573acecc2f72131a79dfaee50fed84d1d1814ec))


## v3.67.4 (2025-09-24)

### Bug Fixes

- Ensure default_factory is mutually exclusive with default in make_all_fields_optional
  ([`554db36`](https://github.com/bec-project/bec/commit/554db36c86290d2c93fcb04abfc7ae2792dccf0e))


## v3.67.3 (2025-09-23)

### Bug Fixes

- Rework signature serializer
  ([`90acf3d`](https://github.com/bec-project/bec/commit/90acf3de76b5c495b27a2fb007aebc85a6a8d4f4))

- **devicemanager**: Add support for nested resolution of auto monitors
  ([`d429e3f`](https://github.com/bec-project/bec/commit/d429e3f5baad1ad71ed74760fffedc1d04f3f5fc))


## v3.67.2 (2025-09-22)

### Bug Fixes

- **fermat_scan**: Add optim_trajectory to movement parameters
  ([`f67f1f4`](https://github.com/bec-project/bec/commit/f67f1f4d6559233efcbfc15d68e3f5b287e7af95))


## v3.67.1 (2025-09-22)

### Bug Fixes

- **file-writer**: Bugfix for non-existing filepath in file_ref
  ([`6584580`](https://github.com/bec-project/bec/commit/65845800d7ca8dd78e1c48cd2c31d462e27befef))


## v3.67.0 (2025-09-15)

### Chores

- Deprecate 3.10, add 3.13
  ([`1be40f2`](https://github.com/bec-project/bec/commit/1be40f2e807b162f1b9608df312b41ae7f2be765))

### Features

- Util function to check plguins
  ([`c375233`](https://github.com/bec-project/bec/commit/c3752337fa9f571eaaf39a84ee50b00123c1954f))


## v3.66.0 (2025-09-12)

### Features

- Change account to redis stream
  ([`3c2e9d4`](https://github.com/bec-project/bec/commit/3c2e9d4b4f19649fbc8149f3c0330b24155628fe))


## v3.65.0 (2025-09-10)

### Documentation

- **user**: Add command for hiding the live table
  ([`25b58f9`](https://github.com/bec-project/bec/commit/25b58f9ead8652c00b3465218917a6454e045a83))

### Features

- Add parameter to scan progress to hide the live table
  ([`4ff1aff`](https://github.com/bec-project/bec/commit/4ff1aff1ae8cbb13d1616633b74ec8863f839f07))

- **scans**: Add kwarg to scan base to disable live table
  ([`f091bb7`](https://github.com/bec-project/bec/commit/f091bb76f188de1e6d8a9ca2b6960df83c3d7b91))


## v3.64.5 (2025-09-10)

### Bug Fixes

- Add autodoc_pydantic to sphinx req and ext
  ([`8571bf7`](https://github.com/bec-project/bec/commit/8571bf7b6beaefb23932205fc7a59b77c21bba0b))


## v3.64.4 (2025-09-09)

### Bug Fixes

- **env**: Make env file names consistent
  ([`2d144fc`](https://github.com/bec-project/bec/commit/2d144fc51a989de54db030981c392a6d9d28bf5d))


## v3.64.3 (2025-09-09)

### Bug Fixes

- Ensure container image exists before e2e tests
  ([`503861d`](https://github.com/bec-project/bec/commit/503861d0a03b86a878f9a23415ccd29e29053252))


## v3.64.2 (2025-09-09)

### Bug Fixes

- **dap**: Fix support for fitting bec history data
  ([`85f5d17`](https://github.com/bec-project/bec/commit/85f5d17adeb12cd675fb892985233706d1092e10))


## v3.64.1 (2025-09-09)

### Bug Fixes

- **atlas**: Scope ingestion messages by stream keys
  ([`fea8c86`](https://github.com/bec-project/bec/commit/fea8c86c96fd464d4449b340740b3f256410def9))

- **scihub**: Unify env var access
  ([`3619a84`](https://github.com/bec-project/bec/commit/3619a8460df3494ca961464a85b9565ed9f2b4e6))


## v3.64.0 (2025-09-03)

### Code Style

- Early returns in hash data algo
  ([`b5a17f5`](https://github.com/bec-project/bec/commit/b5a17f522584ac4493129a644b820704470fdb71))

### Features

- Add a set subclass to manage HashableDevices
  ([`242262b`](https://github.com/bec-project/bec/commit/242262bdea319192318aeacc4204d73bc659b0a6))

- Add device config hashing and variant model
  ([`da85cea`](https://github.com/bec-project/bec/commit/da85ceabf962e7df11e3cacdfbb0d5c0ab43cf5a))

### Performance Improvements

- Cache device hash calculation
  ([`3e38b27`](https://github.com/bec-project/bec/commit/3e38b2758bc810c8ef95dc46055ee95374e98d30))


## v3.63.0 (2025-09-01)

### Documentation

- **plugin_manager**: Add usage docs
  ([`1dc1ec9`](https://github.com/bec-project/bec/commit/1dc1ec9e735c3c748c84a325e8d3321830682e20))

### Features

- **script executor**: Improve message interface for aborted scripts
  ([`8505826`](https://github.com/bec-project/bec/commit/85058265081b7f5313cbbeb89535c92f3445e8f2))


## v3.62.0 (2025-09-01)

### Bug Fixes

- Adjust deployment dir for container utils
  ([`ba6db18`](https://github.com/bec-project/bec/commit/ba6db18ce42ab862b3221fbebfa283c907e97399))

- Deprecation warning
  ([`b8e3e24`](https://github.com/bec-project/bec/commit/b8e3e24eec6b6b1fd3454fa90d88957e54e52abf))

- Remove import risking circularity
  ([`e7fdf69`](https://github.com/bec-project/bec/commit/e7fdf6924808354d74639d6d7cd64b438da53210))

### Code Style

- Fix typo
  ([`db75656`](https://github.com/bec-project/bec/commit/db75656248e4ebfdc5a6b4938d9f393a043d04fa))

### Continuous Integration

- Add pod for procedure containers to e2e test setup
  ([`0d8c202`](https://github.com/bec-project/bec/commit/0d8c202395bca4c7c5804c401f783536a18bc361))

### Features

- Add blocking list pop to RedisConnector
  ([`aadf6f6`](https://github.com/bec-project/bec/commit/aadf6f6fe0cc8b7509974629a0ab61f7cb3231b9))

- Podman CLI backend
  ([`43459c7`](https://github.com/bec-project/bec/commit/43459c7d8651460304d5909fae2d0b7f54174c6d))

- Refactor container utils with generic base
  ([`9d1e601`](https://github.com/bec-project/bec/commit/9d1e601f092cd2b06eb12ce9ce4d875039aac310))

- **procedures**: Add container procedure worker
  ([`7f4379b`](https://github.com/bec-project/bec/commit/7f4379be1ac1fe0336026ae2986d4b99d3ab41f4))

- Adds utilities to build a requirements container image and a worker container image - Adds a
  container based worker running the local python/bec environment (read-only mounted!) - Build the
  worker container from the requirements if necessary

### Testing

- Unit tests for container worker
  ([`20296e2`](https://github.com/bec-project/bec/commit/20296e2fc3556c40bf91cdd4b6dc55cfe0fc7140))


## v3.61.0 (2025-09-01)

### Features

- **scripts**: Add script executor
  ([`6055ca6`](https://github.com/bec-project/bec/commit/6055ca6ddd1a112892d34cba84987764714e6c1a))


## v3.60.0 (2025-08-26)

### Features

- **async writer**: Add support for async signals
  ([`fc98085`](https://github.com/bec-project/bec/commit/fc980850e2ec2de9d98767cbb8a0a30c1d193bf6))


## v3.59.0 (2025-08-26)

### Features

- **scan number**: Sync the scan number and dataset number with the account name
  ([`7e79426`](https://github.com/bec-project/bec/commit/7e794261bd25499653798d6269ec15a9779f4324))


## v3.58.0 (2025-08-25)

### Bug Fixes

- **macros**: Add support for loading macros based on the service config
  ([`4fbba9e`](https://github.com/bec-project/bec/commit/4fbba9e708a514e365e597822f5a0fcd04878910))

### Features

- **user macros**: Ensure macros do not execute code upon loading
  ([`6d282a9`](https://github.com/bec-project/bec/commit/6d282a9aa9c45e9750f0676e261c61beb8c21563))

### Refactoring

- Rename user scripts to user macros
  ([`de4b941`](https://github.com/bec-project/bec/commit/de4b941340592f11af61cd25ee7aac60e0871abb))


## v3.57.0 (2025-08-25)

### Features

- Add option to skip includes in yaml
  ([`cac143c`](https://github.com/bec-project/bec/commit/cac143c2b6286238c191328572395c33e3425403))


## v3.56.0 (2025-08-22)

### Bug Fixes

- **ipython**: Replace deprecated gui.start with gui.show
  ([`a648494`](https://github.com/bec-project/bec/commit/a648494b0f94e042306482d0c67e13949475a631))

### Features

- **service config**: Add support for deployment configs in parent directory
  ([`c8a873b`](https://github.com/bec-project/bec/commit/c8a873b6a7e698921527a7c19b009eb3e09356c3))


## v3.55.2 (2025-08-18)

### Bug Fixes

- **serializer**: Fix signature serialization for literals
  ([`825399a`](https://github.com/bec-project/bec/commit/825399a3ea1aed1457839de6f8dc0c8f26965671))


## v3.55.1 (2025-07-30)

### Bug Fixes

- **device-manager**: Improve failed serialization handling, fix duplicate signal names
  ([`f88eb8b`](https://github.com/bec-project/bec/commit/f88eb8bc824694162ccdf91dd69c1e9cf2b9381f))

- **device-server**: Add InterceptHandler for ophyd logs
  ([`f5a3ee7`](https://github.com/bec-project/bec/commit/f5a3ee7bb28c1787aa33908e4e5beac6fbc9f266))


## v3.55.0 (2025-07-30)

### Bug Fixes

- **gui**: Always create the gui object
  ([`3f0430b`](https://github.com/bec-project/bec/commit/3f0430b7a172985181f0ce4fcdbc65817599a428))

- **service config**: Forward the config name
  ([`c1cb95e`](https://github.com/bec-project/bec/commit/c1cb95ebf7da7018dca9774ba83319ab0389447a))

### Features

- **ipython**: Add option to specify the gui id as cli arg
  ([`b530a8f`](https://github.com/bec-project/bec/commit/b530a8f068ab430347e3083f6da47c512f51c304))

### Refactoring

- **callback-handler**: Add scan history updated callback
  ([`8003eff`](https://github.com/bec-project/bec/commit/8003eff93034dad49691c0bc4d7bed78213c9896))


## v3.54.1 (2025-07-29)

### Bug Fixes

- Build container only on release
  ([`cdd2bdb`](https://github.com/bec-project/bec/commit/cdd2bdb1af3b7cf5b59f9b3da2f62158ca2b58eb))

### Refactoring

- **scan-hisotry**: Add event to indicate that scan history was loaded from redis
  ([`a6d7133`](https://github.com/bec-project/bec/commit/a6d71337a864aeffae0e16eb9e78d330bfd41467))

- **service config**: Restructured service config
  ([`7fd2d0f`](https://github.com/bec-project/bec/commit/7fd2d0f4811dc900d3eafd5874683cdd3be8470e))


## v3.54.0 (2025-07-29)

### Bug Fixes

- **file_writer**: Fix ready to write for empty missing status message
  ([`d7d51a4`](https://github.com/bec-project/bec/commit/d7d51a4ae5b34cf2c0b1f78c5a98316a3e398ec5))

### Features

- **scan guard**: Bypass the scan queue for .get and .read
  ([`4c16351`](https://github.com/bec-project/bec/commit/4c16351eec675b7111ec0f37dc372e3fd3ab0323))


## v3.53.0 (2025-07-29)

### Features

- Build requirements container for multiple archs
  ([`83a62b6`](https://github.com/bec-project/bec/commit/83a62b6a01b8a9b01f4143bc721a459a6acc83bf))


## v3.52.2 (2025-07-29)

### Bug Fixes

- **live table**: Fix check for monitored devices
  ([`b9e7908`](https://github.com/bec-project/bec/commit/b9e79086900d8b8d649bc1e31529dfad9f4d7867))


## v3.52.1 (2025-07-17)

### Bug Fixes

- **device-server**: Use try except for stop call on devices
  ([`5093c1a`](https://github.com/bec-project/bec/commit/5093c1a99017336e57638ff3efaace19ec1a9983))

### Testing

- Add test for stop devices
  ([`9c28142`](https://github.com/bec-project/bec/commit/9c28142d240c12735105d5f6ba576a3a60d69fbc))


## v3.52.0 (2025-07-15)

### Features

- **scan-history**: Add callback for scan history updates
  ([`3922d3f`](https://github.com/bec-project/bec/commit/3922d3f02e47daee081e3746160634bbd51625eb))


## v3.51.0 (2025-07-11)

### Features

- **ci**: Build and push requirements image
  ([`5fb5476`](https://github.com/bec-project/bec/commit/5fb54760c800bff3158c44bf3c5fb2a8950b613d))


## v3.50.2 (2025-07-11)

### Bug Fixes

- **scan queue**: Remove stopped items from the queue
  ([`721d0b8`](https://github.com/bec-project/bec/commit/721d0b82af9b28e84d59a884ecf4b68f61a26e5e))


## v3.50.1 (2025-07-11)

### Bug Fixes

- **scan history**: Filter scan history to readable files
  ([`43a84e3`](https://github.com/bec-project/bec/commit/43a84e32f0f63589ddf84a9fbfb134a4f35cff5a))


## v3.50.0 (2025-07-09)

### Features

- **ScanHistoryMessage**: Add data shape and dtype to ScanHistoryMessage
  ([`824315b`](https://github.com/bec-project/bec/commit/824315b7efa39d1fd8c33625eac4f67b603aa60c))


## v3.49.0 (2025-07-03)

### Features

- **plugins**: Add cli app to manage plugins
  ([`64b7b80`](https://github.com/bec-project/bec/commit/64b7b80282345597f2e31b5726052598a50990d1))


## v3.48.0 (2025-07-01)

### Features

- **alarms**: Report scan id and scan number on alarms
  ([`ccfad65`](https://github.com/bec-project/bec/commit/ccfad6551aed774a357e1791c6917fc2842b02fc))

- **async writer**: Add scan number to constructor
  ([`d0406ae`](https://github.com/bec-project/bec/commit/d0406aee2403122f3c380a36a57286ba4ef6b3a7))

- **scan**: Inject scan number to scan instance
  ([`0d1e879`](https://github.com/bec-project/bec/commit/0d1e879873427237519485f478f9058f490ca918))


## v3.47.1 (2025-06-30)

### Bug Fixes

- Remove 'data' and 'logs'; instead use specified basepath directly
  ([`ee31e58`](https://github.com/bec-project/bec/commit/ee31e5868bff4ce2c55f079b3b3e9785284b2c41))

- **bec service**: Prevent modifications of the default service config
  ([`e07b091`](https://github.com/bec-project/bec/commit/e07b0914df17fca1330643efa762c5422f72daf1))

- **bec-set-account**: Remove compiled script for bec-set-account
  ([`9fee79b`](https://github.com/bec-project/bec/commit/9fee79b873c0b2e558e53688613afbd74115180e))

### Continuous Integration

- Do not run plugin repo tests before finishing core unit tests
  ([`7736f46`](https://github.com/bec-project/bec/commit/7736f46fc4e4ec86caad79147562da959f58ebc0))

- **e2e**: Run tests against branch, not main
  ([`d69d7ea`](https://github.com/bec-project/bec/commit/d69d7ead803c105ad67274b9740faaed7fdd5f05))


## v3.47.0 (2025-06-28)

### Features

- **bec-set-account**: Add go script for account management CLI
  ([`31d5b58`](https://github.com/bec-project/bec/commit/31d5b5856c50d128ef8a79b1afb7217262c24d81))

### Testing

- **bec-set-account**: Add e2e test to ensure functionality of the script
  ([`278d930`](https://github.com/bec-project/bec/commit/278d93055757f87ae87e1fdf71f94d19d30709d5))


## v3.46.0 (2025-06-26)

### Features

- **ipython**: Add support for negative precisions in epics
  ([`2172072`](https://github.com/bec-project/bec/commit/2172072b370edc70ff3b09aa2d5f519c2281f127))


## v3.45.1 (2025-06-26)

### Bug Fixes

- **bec_emitter**: Max must be called on a non-empty set
  ([`01555e0`](https://github.com/bec-project/bec/commit/01555e07df071dc684926b3771cbadd41d44c0ce))

### Continuous Integration

- **plugin-repos**: Add reusable plugin job
  ([`f9043f5`](https://github.com/bec-project/bec/commit/f9043f520277e559cbefa6d1ff995ed7bee1d815))


## v3.45.0 (2025-06-24)

### Continuous Integration

- Add child pipeline for ophyd_devices
  ([`e767d44`](https://github.com/bec-project/bec/commit/e767d446cf16b355cda9a590ddebed30b101ef2c))

- Add GH read only token
  ([`9dfe79d`](https://github.com/bec-project/bec/commit/9dfe79d6f49f025ed2855bd2f159615b4220862d))

- Fix url for plugin repos
  ([`08a1aec`](https://github.com/bec-project/bec/commit/08a1aec694a0d9ff5ac82b194cdedaebd687875b))

- **e2e-action**: Add reusable e2e action for bec
  ([`11dabed`](https://github.com/bec-project/bec/commit/11dabedcb31aae1fac653d5a4fd356caea54622c))

- **plugin-repo**: Add plugin repo action
  ([`ad622d3`](https://github.com/bec-project/bec/commit/ad622d3d043d3a390b4f7200b21a0fd40cbddccb))

### Features

- **bec-server cli**: Add support for shared tmux socket; enable systemd
  ([`aadcde3`](https://github.com/bec-project/bec/commit/aadcde39cef46d99b24139057288b1e9c5301ba9))

### Refactoring

- Fix url to github mirrors
  ([`7ece8fc`](https://github.com/bec-project/bec/commit/7ece8fc7197431d4e63bf083f8b05801ad826579))


## v3.44.2 (2025-06-24)

### Bug Fixes

- **live_table**: Handle precision better
  ([`af303e0`](https://github.com/bec-project/bec/commit/af303e065ab2ecdb6c3565dc942cf655c83f3dbe))

- **live_table**: Warn re: precision at end
  ([`fc018f7`](https://github.com/bec-project/bec/commit/fc018f744e7559c9a4d9d167fad600fd3c06bc73))


## v3.44.1 (2025-06-23)

### Bug Fixes

- Bug in device serialization
  ([`32e8fa4`](https://github.com/bec-project/bec/commit/32e8fa45394c8fd71c6ef6b239cd1c68d4644f9d))

- **bec_service**: Improve error message if service is already running
  ([`40a0579`](https://github.com/bec-project/bec/commit/40a05796c530e84abcd4a974a0719753904748b3))

### Refactoring

- **lazy-wait-for-connection**: Add context manager for lazy-wait-for-connection disable
  ([`c63e455`](https://github.com/bec-project/bec/commit/c63e455d9c605f8e1c36372d153c9e5e217e3198))


## v3.44.0 (2025-06-23)

### Bug Fixes

- **scan history**: Set user id for scan history loader
  ([`61db361`](https://github.com/bec-project/bec/commit/61db361e5d604d377962c7e7c43dc72268af1b7f))

- **scan history**: Shutdown client history thread
  ([`6b431a3`](https://github.com/bec-project/bec/commit/6b431a3990fa161f1872930f14613d484dbceac4))

### Features

- **redis_connector**: Add option to specify a user id to scope xread operations
  ([`f117ddf`](https://github.com/bec-project/bec/commit/f117ddf25917bab553ec95617d2d3144c5e489bd))

- **scan history**: Add request inputs to history message
  ([`98d57cb`](https://github.com/bec-project/bec/commit/98d57cb2c009f4697b8a803b870a9b06ba1560ff))


## v3.43.0 (2025-06-22)

### Features

- **client**: Add cli arg for bec server url
  ([`3f493c5`](https://github.com/bec-project/bec/commit/3f493c599af7d211135c22af6011356eb2c33212))


## v3.42.5 (2025-06-22)

### Bug Fixes

- **serializer**: Reset codec cache when new codecs are registered
  ([`82bbc8c`](https://github.com/bec-project/bec/commit/82bbc8c04e88100957252ca33c29b4135ffaab51))


## v3.42.4 (2025-06-20)

### Bug Fixes

- **async update**: Prevent data emission after scan is closed
  ([`df948db`](https://github.com/bec-project/bec/commit/df948db311f58ca3ccc21e10fed4faef97ad08d1))

- **message_handler**: Use scan_info obj instead of metadata
  ([`17a21fa`](https://github.com/bec-project/bec/commit/17a21fac1ea323c5ec0a35282a37375b9607038a))

- **scan progress**: Do not set to 100 percent on abort; add scan status
  ([`233743c`](https://github.com/bec-project/bec/commit/233743ce064bf7bb6fcfccc8ec104ada4983d598))


## v3.42.3 (2025-06-18)

### Bug Fixes

- Handle none dictionary in config update
  ([`915d062`](https://github.com/bec-project/bec/commit/915d0620b31697a2eeeabd60a1804a053b74419a))

- **message handler**: Fix async signal name to prepend with the device name
  ([`4df4642`](https://github.com/bec-project/bec/commit/4df4642bb9b32d4c4675f847a268e7564485b2b4))


## v3.42.2 (2025-06-16)

### Bug Fixes

- **queue_item**: Safeguard for None in update_with_client_message
  ([`5ebc1dc`](https://github.com/bec-project/bec/commit/5ebc1dc870c4bd45663908d02144b2e22eaf4925))

- **redis_connector**: Send_client_info expire kwarg added
  ([`71790cc`](https://github.com/bec-project/bec/commit/71790cc9b925d2cca15614fa0cd1cf7b1336c9ea))


## v3.42.1 (2025-06-16)

### Bug Fixes

- Ensure unique device tags
  ([`9be9d0d`](https://github.com/bec-project/bec/commit/9be9d0d138e861b5367cbedcc1b153a81d20b5d9))

- Timeout waiting for response
  ([`5de479b`](https://github.com/bec-project/bec/commit/5de479b18e29eaacc6189341f1eee04bc2f45880))


## v3.42.0 (2025-06-16)

### Features

- **client info message**: Add expire flag to client info message
  ([`19cd3b1`](https://github.com/bec-project/bec/commit/19cd3b1ae660c5f02b699c3896cc39ac6bbb3d0b))


## v3.41.0 (2025-06-13)

### Bug Fixes

- **progress_signal**: Progress message must include scan id
  ([`01448ea`](https://github.com/bec-project/bec/commit/01448ea958dce8e1725056892fe74fc54e581552))

### Features

- **bec_service**: Add option to pass in the connector directly
  ([`94ed529`](https://github.com/bec-project/bec/commit/94ed52997b7c60913cf952630d8d646acb54c346))

### Testing

- Fix magic mock for connector validation
  ([`b25a16b`](https://github.com/bec-project/bec/commit/b25a16bd8abe4a95e62b4f6bbdfd0937469c089a))

- **bec_service**: Pass in the connector directly to simplify tests
  ([`20ecdec`](https://github.com/bec-project/bec/commit/20ecdec4ae2334a194abda58b601036979563c13))


## v3.40.0 (2025-06-13)

### Features

- **serializer**: Speed up serialization by avoiding ExtType; add new serialization interface
  ([`6297b1d`](https://github.com/bec-project/bec/commit/6297b1d669248016e1529de3a1a378c39cfab3a3))


## v3.39.1 (2025-06-12)

### Bug Fixes

- **logger**: Avoid re-initializing the connector when configuring the logger
  ([`6187ce3`](https://github.com/bec-project/bec/commit/6187ce362fc948fe99da291cfbf0e8a682c8396c))

### Refactoring

- Expose more config handler function
  ([`2d5498e`](https://github.com/bec-project/bec/commit/2d5498e2868383344b6b17b6037a68dccd3c4dfc))

- Remove deprecated producer and consumer interface
  ([`0c95e9a`](https://github.com/bec-project/bec/commit/0c95e9a6992b4a3a044eb4c2e10cfa982eebcef9))

- **device**: Custom device classes should not inherit the config interface
  ([`d80e413`](https://github.com/bec-project/bec/commit/d80e413d6b46781178d4478507272b8be37632ec))

### Testing

- Add test for device repr
  ([`ee5b81b`](https://github.com/bec-project/bec/commit/ee5b81bcefe5e93bc63e4b15a0401a9dbe743c5b))


## v3.39.0 (2025-06-05)

### Features

- **endpoints**: Publish endpoint info to redis; closes #323
  ([`caa37ad`](https://github.com/bec-project/bec/commit/caa37adefacd265dd1833af3c181d8ad2c087931))

### Refactoring

- Enforce device config keys
  ([`95e0842`](https://github.com/bec-project/bec/commit/95e0842b30e0b9cb7f745601095909821ad58e50))


## v3.38.1 (2025-06-05)

### Bug Fixes

- **device summary**: Show bec signals in separate section
  ([`363c7af`](https://github.com/bec-project/bec/commit/363c7af49c48bba55c380066dbff192cbfc3b812))

### Testing

- Add test for bec signal summary
  ([`842f5a3`](https://github.com/bec-project/bec/commit/842f5a323044e4ead920cbb6bdbde2427c3ad272))

- Fix test object of eiger to be a proper device
  ([`d5e6804`](https://github.com/bec-project/bec/commit/d5e680438e40dec2181afa4b259a9fd853001a54))


## v3.38.0 (2025-06-05)

### Bug Fixes

- **bec signals**: Add default ttl of async signals to 10 minutes
  ([`a25fd08`](https://github.com/bec-project/bec/commit/a25fd08d1e0cf5e54b2493454b73f6afaa9f5370))

- **device serializer**: Emit component name as signal key
  ([`40591b3`](https://github.com/bec-project/bec/commit/40591b3c5146746d87aee74f5ee7b5f39b5f0320))

- **device serializer**: Flatten signal structure for bec signals
  ([`8aa6f13`](https://github.com/bec-project/bec/commit/8aa6f134900c063dc1e78893509c86733d933357))

- **proxy devices**: Do not show device signals where rpc is disabled
  ([`a052e91`](https://github.com/bec-project/bec/commit/a052e91f295e84be05c1de4a3e8a2a240f3952bc))

### Build System

- Increase min dependency of ophyd devices to 1.21
  ([`2678ea6`](https://github.com/bec-project/bec/commit/2678ea6e3efdbd05791cb6999bf1ecc330df1bf1))

- Upgrade min dependency of ophyd devices to 1.20
  ([`06f1c59`](https://github.com/bec-project/bec/commit/06f1c5957d9f6226980bdad8e9c00a76ea026c31))

### Features

- **bec signals**: Add callbacks for progress and file event
  ([`69bfee8`](https://github.com/bec-project/bec/commit/69bfee82d96c4cb0ed7a88e054bfc41bf75ef3f8))

- **bec signals**: Add support for AsyncSignal
  ([`ef8f2b0`](https://github.com/bec-project/bec/commit/ef8f2b0c36342bfa95396e720a31d9b7f77fc9ed))

- **device manager**: Add helper method to retrieve bec signals from config
  ([`75572dc`](https://github.com/bec-project/bec/commit/75572dcaeebc661598bdea45288399bbacb04c78))

- **devicemanager**: Subscribe to preview signals
  ([`a2e895f`](https://github.com/bec-project/bec/commit/a2e895f568c536177cc58f68dcb936d1509f0b88))

- **endpoints**: Add device_async_signal endpoint
  ([`4e6aed0`](https://github.com/bec-project/bec/commit/4e6aed07d09a8ffb08b8eed2dd3523cb9e96cff8))

### Refactoring

- **device server**: Move bec signal update to separate component
  ([`06b8f03`](https://github.com/bec-project/bec/commit/06b8f03908652d1a90c7988b756accc05e938b3b))

### Testing

- Add device with bec signals to test config
  ([`d470451`](https://github.com/bec-project/bec/commit/d47045133a53a64972e217546089f2e6174972b7))

- Add test for bec signals retrieval
  ([`73e7366`](https://github.com/bec-project/bec/commit/73e73667ab67fdd119f93a360c677c629652a79e))

- Add test for missing scan ids
  ([`64ed69e`](https://github.com/bec-project/bec/commit/64ed69e6b680c96bae6c8889508ddc125e206191))

- **device_manager**: Add test for async signals forwarding
  ([`fe6e0ee`](https://github.com/bec-project/bec/commit/fe6e0ee922d1c97448e6c359d8d8c7c16ca2a2d4))


## v3.37.3 (2025-06-05)

### Bug Fixes

- Use github repo for plugin template tests
  ([`d2e457e`](https://github.com/bec-project/bec/commit/d2e457eff95e57933953287b1e413ad87bd0f734))

### Refactoring

- Delete old plugin generation scripts
  ([`42989fb`](https://github.com/bec-project/bec/commit/42989fba8d9d3242bc3df2a742ee9a71e253edc6))


## v3.37.2 (2025-06-03)

### Bug Fixes

- **bec-service**: Add logging for redis host during init of bec-service; closes #476
  ([`5091cc2`](https://github.com/bec-project/bec/commit/5091cc24f6c00a1b3d3f55dcfbac7a827adfbbe1))

- **bec-yaml-loader**: Add support for relative filepaths; closes #483
  ([`fc45e54`](https://github.com/bec-project/bec/commit/fc45e54f1307edbf9c32d94f37f36bccc64534e1))

- **device-serializer**: Support get_device_info for components with lazy=True, closes #458
  ([`964bae1`](https://github.com/bec-project/bec/commit/964bae112cdc2b3e999b5d8e78704c28c8ab9257))

### Refactoring

- **device-serializer**: Improve docstring
  ([`fd901c9`](https://github.com/bec-project/bec/commit/fd901c94e818cc00475b2ea72b5fd3bc04b2eacc))


## v3.37.1 (2025-06-01)

### Bug Fixes

- **device_server**: Remove unnecessary imports in init to avoid shadowing the import of environment
  setups
  ([`40798bc`](https://github.com/bec-project/bec/commit/40798bca5f656b8be5204a94f8883c208bc4422a))

### Testing

- Fix import of device server
  ([`c111f99`](https://github.com/bec-project/bec/commit/c111f991cb481266124f2a168ae92e7a09827004))


## v3.37.0 (2025-05-30)

### Bug Fixes

- **file event**: Only emit file if file is not in append mode
  ([`f70f33a`](https://github.com/bec-project/bec/commit/f70f33aadf9e494530f1527c4bc586e2d821ba6c))

- **FileMessage**: Fix signature and validation
  ([`c5d1832`](https://github.com/bec-project/bec/commit/c5d18320b7f6c4d1ce84245d39a325bb1d121be6))

- **hdf5 plugins**: Add support for loading files with h5 filters
  ([`0b7c3df`](https://github.com/bec-project/bec/commit/0b7c3dfb2ae7fefff58dd0514438e56104a6aeb7))

### Documentation

- Add comment to unused import of hdf5plugin
  ([`eb09831`](https://github.com/bec-project/bec/commit/eb098317a5edf66fcb610bc733f4ce035c86eedc))

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>

### Features

- **file references**: Add file references to collection
  ([`c64d71e`](https://github.com/bec-project/bec/commit/c64d71ea54483f89509c7ad253ceb4dc4c370db1))

### Refactoring

- **device_manager**: Always set is_master_file to False outside of the file writer
  ([`d98117f`](https://github.com/bec-project/bec/commit/d98117f3e9f09cc76aecf32708f6c0db6a04a4dd))

### Testing

- Extend storage mock to cover file references
  ([`efe3db7`](https://github.com/bec-project/bec/commit/efe3db73a34ee3f468e0924c4c61924f733c46ec))


## v3.36.0 (2025-05-30)

### Features

- **device_serializer**: Add signal class to device serializer
  ([`68a01fd`](https://github.com/bec-project/bec/commit/68a01fdce6524929da1dfa22674065bf81ebb0ed))

- **messages**: Add new endpoints and messages for device_preview and device_user_roi
  ([`8755e67`](https://github.com/bec-project/bec/commit/8755e6746eb5870bd638e906fda3543e0f40369e))


## v3.35.11 (2025-05-28)

### Bug Fixes

- **test**: Dangling client thread
  ([`38f954e`](https://github.com/bec-project/bec/commit/38f954e31543c09e53bf1b061f184b532905a981))


## v3.35.10 (2025-05-28)

### Bug Fixes

- **file_writer_manager**: Fix file writer read_to_write if no monitored devices are in the config;
  closes #481
  ([`df875d3`](https://github.com/bec-project/bec/commit/df875d3d5391ff88d132353890a916b9bebb65fb))

- **live-table**: Fix live table updates for scan_report_type scan_progress without monitored
  devices
  ([`6e13ba3`](https://github.com/bec-project/bec/commit/6e13ba35c88e7aaaafb9d7e55f7127e3d49fc289))

### Testing

- **file-writer-manager**: Fix test for file writer manager ready_to_write
  ([`29370fb`](https://github.com/bec-project/bec/commit/29370fbb178c2cd6d1342d07d0519110441f9f1a))

- **live-table**: Improve live table test coverage; test_run_update
  ([`b8faf33`](https://github.com/bec-project/bec/commit/b8faf33260ce5f053f4ec5132822cc8f6753feaf))


## v3.35.9 (2025-05-28)

### Bug Fixes

- **rpc**: Add obj to status base instances
  ([`3c13af4`](https://github.com/bec-project/bec/commit/3c13af43a403654f00cea91e8bd14d2073024a8e))


## v3.35.8 (2025-05-28)

### Bug Fixes

- Scanrequestmessage metadata dict structure
  ([`9699aa0`](https://github.com/bec-project/bec/commit/9699aa043d90b1037851ed701c19171725670cc6))

### Documentation

- Changed gitlab references to github
  ([`b8608fb`](https://github.com/bec-project/bec/commit/b8608fb5fcfb0b29ff3ce5e61948c0ceeb09b496))


## v3.35.7 (2025-05-26)

### Bug Fixes

- **scans**: Close scans explicitly through close_scan
  ([`34ceaa5`](https://github.com/bec-project/bec/commit/34ceaa55fdf258c847180639d0a21067ecdb8a6a))

### Chores

- Add issue templates
  ([`8a31d02`](https://github.com/bec-project/bec/commit/8a31d02c1e355d8ca759fa85441b612c93f76b0a))

- Add pull request template
  ([`fba2435`](https://github.com/bec-project/bec/commit/fba243505d46e1808fb9db89fb46eb6f9130d8b5))

### Testing

- Add test for backward-compatible cleanup
  ([`2031014`](https://github.com/bec-project/bec/commit/2031014d4dad04ad81197357aec3a967a5cef5b8))

- Fix flaky queue test
  ([`1c968a2`](https://github.com/bec-project/bec/commit/1c968a26f9d956f2d9f6bdf940418f54b0e6cfb9))


## v3.35.6 (2025-05-24)

### Bug Fixes

- Add missing dotenv dependency
  ([`4cf9129`](https://github.com/bec-project/bec/commit/4cf9129531699641031f59d0812ba3ccc6184e4f))

### Chores

- **license**: Update copyright year to 2025
  ([`51f986c`](https://github.com/bec-project/bec/commit/51f986cbba702ea08f4714c941878ae3190e611c))

### Continuous Integration

- Add github actions
  ([`7bf91eb`](https://github.com/bec-project/bec/commit/7bf91eb47c22e7505cf1bcbdedb43e2c0e5256e0))

- Add pr issue sync
  ([`4a90e60`](https://github.com/bec-project/bec/commit/4a90e60436c140f7a09c2e76062bc75870958835))

- Add semver job
  ([`bce568b`](https://github.com/bec-project/bec/commit/bce568b213675d19f45146cefb121bf6fd5f534f))

- Auto-add issues to project
  ([`6da4f14`](https://github.com/bec-project/bec/commit/6da4f1425bb932bcc8544aaf2b9d753385dd9a5b))

- **workflow**: Update add-to-project action version to v1.0.2
  ([`176e528`](https://github.com/bec-project/bec/commit/176e52881c8a15bb5fcf655834cb224c93ae587e))

### Refactoring

- **config**: Tidy typing
  ([`2116d6f`](https://github.com/bec-project/bec/commit/2116d6f72b245d0cfb4354412959c9af43e4d103))


## v3.35.5 (2025-05-21)

### Bug Fixes

- **plugin_helper**: Avoid crashing on misconfigured plugins
  ([`915a299`](https://github.com/bec-project/bec/commit/915a29954f341bb833f22f03d3ccf962f5674d5d))


## v3.35.4 (2025-05-21)

### Bug Fixes

- **config**: Minor improvements to add/remove device configs in runtime
  ([`59d9b88`](https://github.com/bec-project/bec/commit/59d9b88287ccd8197b139d32bdfa8d5d43a17cf6))

### Continuous Integration

- Disabled dev pages
  ([`4288fb0`](https://github.com/bec-project/bec/commit/4288fb04dc6b52af3b8eeecb251bdfb1034e8a24))

### Testing

- **config handler**: Make sure new device does not exist yet
  ([`c5604c3`](https://github.com/bec-project/bec/commit/c5604c3e6a78d1f3fa8ac086abda6a6a762eda8f))


## v3.35.3 (2025-05-20)

### Bug Fixes

- **async_writer**: Do not clear the status event on init
  ([`0a4324c`](https://github.com/bec-project/bec/commit/0a4324ced9a8d7a23c7a2621329f622f68ac4ad5))


## v3.35.2 (2025-05-18)

### Bug Fixes

- **client**: Extend wait time for server restart to ensure proper service initialization
  ([`02166a0`](https://github.com/bec-project/bec/commit/02166a07c686802e0c907966e584cdac2a0f2c35))


## v3.35.1 (2025-05-13)

### Bug Fixes

- **BECIPythonClient**: Allow setting MagicMock attributes directly
  ([`ca2cedb`](https://github.com/bec-project/bec/commit/ca2cedb673b486d871cfb15eba726d68cd69c696))

- **BECIPythonClient**: Resolve to _client for setting attributes
  ([`327170e`](https://github.com/bec-project/bec/commit/327170ed828730c485582dc0bd1ec0902372ec30))

### Documentation

- **BECIPythonClient**: Add documentation for _local_only_types attribute
  ([`8400973`](https://github.com/bec-project/bec/commit/840097325c420e37d5833cae6c54b8e5db7134ba))


## v3.35.0 (2025-05-13)

### Bug Fixes

- **acl_login**: Enhance error message for missing login information in ACL setup
  ([`037a674`](https://github.com/bec-project/bec/commit/037a674af66522e65be4508a297f14420d2c3f7e))

- **acl_login**: Rename user variable to username and update user_service_login method to accept
  username parameter
  ([`2dd5285`](https://github.com/bec-project/bec/commit/2dd52851fea8f3bc725265929378a86c556ad8d8))

- **atlas**: Emit profile for test accounts
  ([`0ac2eb3`](https://github.com/bec-project/bec/commit/0ac2eb3496ca535e6bbb60dbede9b53f2f5e2211))

- **bec_service**: Replace _auto_login with _bec_service_login
  ([`d37db73`](https://github.com/bec-project/bec/commit/d37db732a8dc431f004c65d67b29b5cebe1b9362))

- **client**: Use acl user, not system user as username
  ([`a8907c8`](https://github.com/bec-project/bec/commit/a8907c82718462a86f5cb104cc46e796645c5a18))

- **ipython**: Cleanup; improved error reporting when user is unauthorized
  ([`62a7453`](https://github.com/bec-project/bec/commit/62a7453f03f8ca61cfa1e2d8ce7c3f6b10b85c9e))

- **redis_connector**: Ensure default password is set to "null" when None is provided
  ([`b8bfa51`](https://github.com/bec-project/bec/commit/b8bfa51e28244796f5cb0699da9588681af78b8a))

- **redis_connector**: Improved permission error handling for redis connector
  ([`18058c2`](https://github.com/bec-project/bec/commit/18058c264547ddb38fa26d6705d332022f368c61))

### Chores

- **.gitignore**: Add .env files to the ignore list
  ([`c449527`](https://github.com/bec-project/bec/commit/c449527f26e3b7b4dc9c0f0ef2a7b761c8dfd0bb))

- **redis_connector**: Add None as type hint to the authenticate method
  ([`3d6ee28`](https://github.com/bec-project/bec/commit/3d6ee281043ce5962ee522661e01e403607a93d5))

### Features

- **ACL**: Added basic support for Redis ACLs
  ([`9816066`](https://github.com/bec-project/bec/commit/9816066de1729d78922c79e73cb179488de3a560))

- **bec_lib**: Added acl related messages and endpoints
  ([`dd01e1d`](https://github.com/bec-project/bec/commit/dd01e1d93cf11c229bc3dadf003a891fce36f57a))

- **bec_service**: Add user argument to command line parser for service configuration
  ([`afaa449`](https://github.com/bec-project/bec/commit/afaa449d550e7d40365cb5496c7f141626f50cf4))

- **ipython**: Add su ipython magic
  ([`f74caba`](https://github.com/bec-project/bec/commit/f74caba8dfb78ea55d5bc638293953d7c124f807))

- **ipython**: Add username to cli prompt
  ([`3032960`](https://github.com/bec-project/bec/commit/3032960b22be83c2c9c4c0f3a663e447f5d06091))

- **scihub**: Publish acl info on startup
  ([`61658f7`](https://github.com/bec-project/bec/commit/61658f793ebeb7668acfce7b3a9ef1383e1d63cf))

- **user_acls**: Implement add_account method for user management with access levels
  ([`0000e74`](https://github.com/bec-project/bec/commit/0000e7460cf4422944f24f391b5a0d300b97fc24))

### Refactoring

- Improve http error handling
  ([`ae7711f`](https://github.com/bec-project/bec/commit/ae7711f6cab0edc810c9ae6cfc65798b0ab8b36a))

- Moved acl logic to acl_login
  ([`50a948f`](https://github.com/bec-project/bec/commit/50a948f3cb8e743a55bff2c2e17ab446366b8c28))

- **acl**: Remove user bec
  ([`bcbc6a3`](https://github.com/bec-project/bec/commit/bcbc6a303a50c302909657cab600ff222ea75b4c))

- **redis_connector**: Update authenticate method to use keyword-only arguments
  ([`45c3373`](https://github.com/bec-project/bec/commit/45c3373dbc596146eaaa4ad5c9eea5beadfb44e1))

- **user_acls_test**: Remove deprecated add_bec and add_bec_limited methods
  ([`86cfb85`](https://github.com/bec-project/bec/commit/86cfb85f940001ad48a425b55fea6cacdf70053b))

### Testing

- Refactor login tests to use access_control fixture for account management
  ([`d4aef30`](https://github.com/bec-project/bec/commit/d4aef302cdf02529387a67a4aea8d91a2eb394ba))

- **acl**: Fixed tests after acl changes
  ([`183e2b7`](https://github.com/bec-project/bec/commit/183e2b74b68e58e97ce32d969b6ca3dbdb7da99e))


## v3.34.0 (2025-05-09)

### Features

- Add helper functions for plugin template
  ([`5a1d811`](https://github.com/bec-project/bec/commit/5a1d81113353d4abba057a2f042208a9df87f72a))


## v3.33.0 (2025-05-06)

### Features

- **scan_report**: Add num_points and file_written flags to wait method
  ([`f4388c8`](https://github.com/bec-project/bec/commit/f4388c8bfee90ff0eff17fd5c705352128781663))

### Testing

- **e2e**: Update wait method to include num_points and file_written flags
  ([`ee384c3`](https://github.com/bec-project/bec/commit/ee384c3e8b3af1bf0474d7c5ca254e5692e72702))


## v3.32.0 (2025-05-06)

### Bug Fixes

- **file_writer**: Transition to get_full_path
  ([`6d4982a`](https://github.com/bec-project/bec/commit/6d4982a15fc5530cb05438bfa6da70883e28af2a))

- **scan_worker**: Add account string validation
  ([`4c93982`](https://github.com/bec-project/bec/commit/4c93982766acf0a0457be0ee63e6205c7f91c843))

### Features

- **ScanWorker**: Implement dynamic file base path retrieval with template support
  ([`c35622b`](https://github.com/bec-project/bec/commit/c35622b1f88274c2696d2794b12772f12b1cf271))

### Refactoring

- Minor type hints improvements
  ([`94f48e3`](https://github.com/bec-project/bec/commit/94f48e33f88d93f4184bdfaabccf5d9b75d565c8))

- **file_utils**: Move default "data" directory from file_utils to scan worker
  ([`71caefd`](https://github.com/bec-project/bec/commit/71caefda79873317e900ef2eaa63c9291c512146))


## v3.31.1 (2025-05-05)

### Bug Fixes

- **scan_queue**: If queue is empty and paused, wait for it to resume before injecting new scans
  ([`d668961`](https://github.com/bec-project/bec/commit/d6689611861a6c23cd0c89a654fe0d216e63ae16))

### Testing

- **image_analysis**: Increase position tolerance
  ([`7e65625`](https://github.com/bec-project/bec/commit/7e656254f2ab386b939f94e43a9d658b90cddb4a))


## v3.31.0 (2025-05-02)

### Bug Fixes

- **data_access**: Fix to_pandas conversion for baseline devices
  ([`f22f343`](https://github.com/bec-project/bec/commit/f22f343db03ac2a9b5de61396c2994296e80c49c))

### Documentation

- **history access**: Add section on .get and .data access
  ([`5eec001`](https://github.com/bec-project/bec/commit/5eec001ea6258c44e1041f8c4f3bfb886c77a6cf))

### Features

- **history**: Add data collection for direct signal access as well as .get access pattern
  ([`538c243`](https://github.com/bec-project/bec/commit/538c2434602910dd7d842eba8b08cc35c7f18820))


## v3.30.0 (2025-05-02)

### Chores

- **formatter**: Upgrade to black v25
  ([`c1b49a0`](https://github.com/bec-project/bec/commit/c1b49a0c86a16935a09353665751cbff3806c239))

### Features

- **IPython**: Add magic to restart the server
  ([`aba1b31`](https://github.com/bec-project/bec/commit/aba1b31b91925144a4752dc461cd660fce88de54))


## v3.29.2 (2025-05-01)

### Bug Fixes

- **HDF5FileWriter**: Auto-select plugin if only one exists
  ([`66892c5`](https://github.com/bec-project/bec/commit/66892c5650a90d6f160dd8d14f944b2ea29d81b8))

- **HDF5Storage**: Return existing group if already created in create_group method
  ([`7763761`](https://github.com/bec-project/bec/commit/77637615214089cf948fad4e14da4dabcbb26b1b))

### Refactoring

- **DefaultFormat**: Move bec-specific methods outside of .format method and always call it
  ([`46497b6`](https://github.com/bec-project/bec/commit/46497b6dc2a50983c059e2d6ee8699986cdf9f80))

### Testing

- **file_writer**: Fixed tests after refactoring
  ([`ad04b45`](https://github.com/bec-project/bec/commit/ad04b45a32ee9d0f7af844e0be3694c1b26f635c))


## v3.29.1 (2025-04-25)

### Bug Fixes

- Log and discard errors on loading user scripts
  ([`cc68581`](https://github.com/bec-project/bec/commit/cc685819925ffec6fb912be730daa36043536e71))


## v3.29.0 (2025-04-22)

### Continuous Integration

- Fixed conda e2e test
  ([`4a9823c`](https://github.com/bec-project/bec/commit/4a9823ca156164f052c7d7b86312ebc440b4f027))

- Redis is called redis-server on conda forge
  ([`0c31056`](https://github.com/bec-project/bec/commit/0c310569891b2fd788c9de0c55af3fd74c727a8c))

### Features

- **messages**: Add `container_proxy` to GUIRegistryStateMessage state definition
  ([`716c42b`](https://github.com/bec-project/bec/commit/716c42b5408038daf174e80a48273c14c3321589))

### Refactoring

- **messages**: Add `object_name` to GUIRegistryStateMessage
  ([`f95bda1`](https://github.com/bec-project/bec/commit/f95bda1f2afb9888c9cea89dc8076d8666e8f410))

name will be deprecated in the future


## v3.28.1 (2025-04-01)

### Bug Fixes

- Replaced sleeps with event waits
  ([`d3ee802`](https://github.com/bec-project/bec/commit/d3ee80256b81f6fcf0d9a05f9a71ade4cedbaafc))

### Refactoring

- Minor type hints improvements
  ([`32b136a`](https://github.com/bec-project/bec/commit/32b136aa21f972580d4a896468abe289b96725d2))


## v3.28.0 (2025-03-27)

### Features

- **service_handler**: Add interface argument for process management options
  ([`5b96554`](https://github.com/bec-project/bec/commit/5b9655457b6c74e5ba1d6718b3fdda82bfbadb96))


## v3.27.0 (2025-03-26)

### Bug Fixes

- **device_manager**: Only publish configuration if it is not a signal
  ([`a045226`](https://github.com/bec-project/bec/commit/a045226efeb7ae885678ad32063e019197cf1ffb))

### Features

- **file_writer**: Add support for writing configuration data to the master file
  ([`4919b47`](https://github.com/bec-project/bec/commit/4919b4781a9e2714673f81ce9d4a0038c0026b60))

### Refactoring

- Type hint improvements
  ([`c1a2bd2`](https://github.com/bec-project/bec/commit/c1a2bd23693a1378866c4629b32b447e55223ecf))

- **scan_guard**: Enhance documentation for exception handling and class purposes
  ([`bceebb7`](https://github.com/bec-project/bec/commit/bceebb768c3e6ed4402e09143a2b1c036302669f))

### Testing

- **file_writer**: Add unit test for extracting the device name from the topic
  ([`3b12889`](https://github.com/bec-project/bec/commit/3b128894c91a8037f5e3c1d9898c1600bbfdb14a))

- **file_writer**: Extend file writer e2e test to check that configuration signals are updated
  ([`8202fcb`](https://github.com/bec-project/bec/commit/8202fcb2956fbd05e385ff56cfce7f22214cd24e))


## v3.26.0 (2025-03-26)

### Features

- **rpc**: Rpc objects should have the original class name
  ([`1cfa35b`](https://github.com/bec-project/bec/commit/1cfa35ba3bbecba9eaee8936692ff68f706f61c9))

### Testing

- Update mock data for device_class info
  ([`11dfced`](https://github.com/bec-project/bec/commit/11dfcedbc0ff49bf1d2b817cfc287cae1b8b67ab))


## v3.25.2 (2025-03-26)

### Bug Fixes

- **service_handler**: Default to tmux interface due to auth error with systemctl
  ([`06f645a`](https://github.com/bec-project/bec/commit/06f645a018dc700d81a7a9932b38182a5d150f87))


## v3.25.1 (2025-03-22)

### Bug Fixes

- **async_writer**: Get the readout_priority from the scan message, not the device config
  ([`0d7dc5d`](https://github.com/bec-project/bec/commit/0d7dc5d3ae4b28bf55fe931928de43b06cb18b3c))

- **scan_stubs**: Respect readout priority modifications for the stage order
  ([`8e8cf2f`](https://github.com/bec-project/bec/commit/8e8cf2fda4a5e2218a0447da8df86759b68e433f))


## v3.25.0 (2025-03-20)

### Features

- **bec_server**: Added support for interacting with bec-server commands through systemd
  ([`6d36184`](https://github.com/bec-project/bec/commit/6d361840cc28bd2922fad5eae70024f58d5ce470))


## v3.24.1 (2025-03-18)

### Bug Fixes

- **config_handler**: Handle None deviceConfig by initializing to empty dict
  ([`5f34284`](https://github.com/bec-project/bec/commit/5f342845de42800003a1c901db28e972368fa14a))

### Refactoring

- **file_writer_manager**: Add logging for scan segment processing readiness
  ([`edad81e`](https://github.com/bec-project/bec/commit/edad81efe536496ca64ef712c2fbe5f322218fe2))


## v3.24.0 (2025-03-18)

### Bug Fixes

- **ci**: Update pytest command to include files path for end-to-end tests
  ([`0caf525`](https://github.com/bec-project/bec/commit/0caf525b5c1de81350bba43a5dded03643d34d3d))

- **device_server**: Update device only after adding it to the DM
  ([`0639ced`](https://github.com/bec-project/bec/commit/0639ced19b1146a249b40093a8823b5264bdcb77))

- **device_server**: Update instruction immediately after adding status object
  ([`db9173f`](https://github.com/bec-project/bec/commit/db9173f5973b69cf78185c459f2adf113600fec7))

- **redis_connector**: Move shutdown of generator executor to end of the shutdown procedure
  ([`5557690`](https://github.com/bec-project/bec/commit/5557690d846b82a7d8398a6d7bb20006a94234bd))

- **request_handler**: Prevent ValueError when request ID is not found
  ([`24402f1`](https://github.com/bec-project/bec/commit/24402f1cb78badeca76993428410693c52f8b065))

- **scan_stub_status**: Include 'done' status in string representation
  ([`fd1803a`](https://github.com/bec-project/bec/commit/fd1803abc50b2f69f69ca920eab5925fca6c3e57))

### Chores

- **device_manager**: Add logging for failed device initialization
  ([`27d2912`](https://github.com/bec-project/bec/commit/27d29128071924ad4da6268772ee446fc9f45647))

### Continuous Integration

- Removed unnecessary config for nightly e2e tests
  ([`3e55d44`](https://github.com/bec-project/bec/commit/3e55d44e4fe1595824f9f1c1fe04c7a0943bba6d))

### Features

- **monitor_scan**: Add min_update parameter for configurable update interval
  ([`dc349a3`](https://github.com/bec-project/bec/commit/dc349a3860378ef4f86f1919de81ef2de91a81ee))

### Refactoring

- Split procedure manager module
  ([`9d1e152`](https://github.com/bec-project/bec/commit/9d1e1526efe3795df0e30e6e5fe2e7abad361ce0))

- **scan_storage**: Optimize handling of pending inserts for scan updates and segments
  ([`f50a207`](https://github.com/bec-project/bec/commit/f50a2076e8da1de4e96f0b54daa3efa7080486d2))

### Testing

- **e2e**: Add min_update parameter to monitor_scan in end-to-end test
  ([`14a388b`](https://github.com/bec-project/bec/commit/14a388bf4fdd6adcde551c0d1a1e65d783ce5aa1))

- **scan_stubs**: Update ScanStubStatus representation to include 'done' state
  ([`49cbec6`](https://github.com/bec-project/bec/commit/49cbec6f554d70a24ae867701c29198fc7de9c17))


## v3.23.0 (2025-03-14)

### Documentation

- **atlas_models**: Add docstring to Device model for clarity on usage
  ([`51a914e`](https://github.com/bec-project/bec/commit/51a914e0d2b14122b9c7115f03b6516bef15c2af))

### Features

- **atlas_models**: Add BEC Atlas models
  ([`727e7f8`](https://github.com/bec-project/bec/commit/727e7f89a5ffa49c8d86bd56c097cadd7b88cbe8))

- **config_handler**: Replace SciBecValidator with Pydantic models for device validation
  ([`9a1e77d`](https://github.com/bec-project/bec/commit/9a1e77db556fcda9c2a2b17fef69ceaf62304ccb))

### Refactoring

- **sci_bec_validator**: Add deprecation warning for SciBecValidator usage
  ([`b399f4b`](https://github.com/bec-project/bec/commit/b399f4b2f3a4bf19586a612cb1452cb8cb57effd))


## v3.22.3 (2025-03-14)

### Bug Fixes

- **serialization**: Generalized support for serializing Device objects
  ([`9cbd702`](https://github.com/bec-project/bec/commit/9cbd7028f531318f77f289284b2898a3479ef709))


## v3.22.2 (2025-03-13)

### Bug Fixes

- **signature_serializer**: Fix dtype serialization and deserialization for Union, NoneType and
  numpy
  ([`c667c06`](https://github.com/bec-project/bec/commit/c667c0694d62bcfac156ff7f15e216744a9cbf0b))

- **signature_serializer**: Update dtype deserialization to use Union for multiple types
  ([`d9cd1c7`](https://github.com/bec-project/bec/commit/d9cd1c7d66e72a593609c1961608e1609e47161f))

### Continuous Integration

- Fix conda channel for PSI policy change
  ([`fee67e9`](https://github.com/bec-project/bec/commit/fee67e98e71a1cb58969410f1ebfacdc3987922b))


## v3.22.1 (2025-03-12)

### Bug Fixes

- **endpoints**: Ensure enums are correctly converted to strings across all supported python
  versions
  ([`e273b83`](https://github.com/bec-project/bec/commit/e273b83efae5e01ace1e51bd8e50088dd31e8475))

- **file_writer_manager**: Update ready_to_write logic to ensure all points are received
  ([`1cc94bf`](https://github.com/bec-project/bec/commit/1cc94bf36e01e147301ed0826ea545fcc79b7058))


## v3.22.0 (2025-03-12)

### Bug Fixes

- **rpc**: Enhance custom user access with function signatures
  ([`a3bcb27`](https://github.com/bec-project/bec/commit/a3bcb27c4b01c41915d16429603a78820551505c))

### Features

- **tests**: Enhance mock device info with detailed function signatures
  ([`f8d196e`](https://github.com/bec-project/bec/commit/f8d196eab5a637b8dfbcd7d32dfe7202fb0fdd19))


## v3.21.1 (2025-03-12)

### Bug Fixes

- **device_serializer**: Component doc can be None
  ([`0fedd25`](https://github.com/bec-project/bec/commit/0fedd258383c642bb508b7e1361a632541de6c4e))

- **device_serializer**: Only call ophyd methods when connected as they may raise
  ([`3bd9095`](https://github.com/bec-project/bec/commit/3bd9095be052f358c63fba8d0d5183d2a99c9b87))

- **file_writer**: Fix writing of async data
  ([`006d65d`](https://github.com/bec-project/bec/commit/006d65d2cddd01be9abdd8524bd0a2226c2be344))


## v3.21.0 (2025-03-10)

### Bug Fixes

- Coverage concurrency settings
  ([`494df6c`](https://github.com/bec-project/bec/commit/494df6cd968b8e2bd5c563d21c8e1ceebfb29687))

### Features

- Add procedure manager and worker
  ([`f9c299a`](https://github.com/bec-project/bec/commit/f9c299af38a3a94e54a36502f1aff6497494aefe))

Adds the basic things needed for procedures: - add message and endpoint types: - rename
  MessageOp.SET to MessageOp.KEY_VALUE - add MessageOp entries for set operations - add functions in
  redis_connector: - to interact with clients - to interact with sets - add procedure infra: -
  manager distributes request queue to workers - worker base class which times out when its queue is
  empty for too long - simple runner for testing


## v3.20.0 (2025-03-10)

### Features

- **endpoints**: Add GUI ACL endpoint for credential exchange
  ([`8e3618c`](https://github.com/bec-project/bec/commit/8e3618ce04b78fc221cd29fd14caa468ab569dc2))

### Testing

- **async_writer**: Add data consistency test for fixed size slices
  ([`c684e6b`](https://github.com/bec-project/bec/commit/c684e6b827119008ce42a376fc5e2fa7a14291d2))


## v3.19.0 (2025-03-07)

### Bug Fixes

- **device_server**: Add thread safety to request handling with a lock
  ([`42a9ef2`](https://github.com/bec-project/bec/commit/42a9ef2c30591a824b2eca876e88545b0cd75431))

### Features

- Add option to receive DeviceStatus for pre_scan
  ([`4746281`](https://github.com/bec-project/bec/commit/4746281501ea47443bcba4b154ec4d2a64f209f3))

### Refactoring

- Opptimize status handling if no DeviceStatus is returned
  ([`b900249`](https://github.com/bec-project/bec/commit/b90024949091641913b825cce8c7e468d5d02f45))

- Switch checks to ophyd.StatusBase instead of ophyd.DeviceStatus
  ([`1ec2efe`](https://github.com/bec-project/bec/commit/1ec2efe9216ec3b069e59846663d54c49b9174dc))


## v3.18.0 (2025-03-06)

### Features

- Add GUI registry state endpoing and message
  ([`287fd84`](https://github.com/bec-project/bec/commit/287fd84818b6854f3f6b4da2afa558e3a899f22f))

### Testing

- Add test for message & type hint improvements
  ([`4902101`](https://github.com/bec-project/bec/commit/4902101aabe6812b85f7ba0f97920970a4637121))


## v3.17.1 (2025-03-04)

### Bug Fixes

- **device_server**: Include error message in set_finished on exceptions
  ([`242a2f0`](https://github.com/bec-project/bec/commit/242a2f085322881e9182e5871b8f075ffd192553))


## v3.17.0 (2025-03-03)

### Features

- **scan_history**: Add __len__ method to ScanHistory class
  ([`736ff5d`](https://github.com/bec-project/bec/commit/736ff5d33dbbfade4a510d2e676da7a271b4c323))

### Refactoring

- **bec_lib**: Added pragma no covers
  ([`bd983af`](https://github.com/bec-project/bec/commit/bd983aff0a1768c147aacb2926fbf5b399ddf48d))

- **bec_lib**: Improved type hints across bec_lib
  ([`91d5c38`](https://github.com/bec-project/bec/commit/91d5c38a4abf8d1022edf341e64d6443574b3649))

- **type hints**: More type hints improvements for redis connector
  ([`1d8792f`](https://github.com/bec-project/bec/commit/1d8792fdc5f71174d9eb2a155f3aa4a19e082b67))

- **type hints**: Moved from ConnectorBase to RedisConnector
  ([`53ccab1`](https://github.com/bec-project/bec/commit/53ccab11505e40bb60914ae0bf78c3b4710d4e05))


## v3.16.1 (2025-02-28)

### Bug Fixes

- **redis_connector**: Minor changes for type hints on redis connector
  ([`1579516`](https://github.com/bec-project/bec/commit/15795161b2bccb3dd95598b3a194a74a9b4ff589))

### Refactoring

- **bec_lib**: Removed async data handler
  ([`c91666f`](https://github.com/bec-project/bec/commit/c91666f383bd37e39188743339d5d4db2c5f13d9))


## v3.16.0 (2025-02-27)

### Bug Fixes

- **device**: Fixed kind access for .read
  ([`79421ac`](https://github.com/bec-project/bec/commit/79421acbc82a238cad528a1746a07453bddf26f3))

- **kind**: Fixed kind serialization for py311+
  ([`d30df18`](https://github.com/bec-project/bec/commit/d30df186ad2ea8fe8c448b43079d47ac1ef331e8))

### Features

- **client**: Custom summary for devices including docs
  ([`2c530bf`](https://github.com/bec-project/bec/commit/2c530bfbb76567fd040ab0e9b6af74c52b95ab4c))

- **device_server**: Add component doc and describe to device info
  ([`99173c0`](https://github.com/bec-project/bec/commit/99173c02fd7e7317a7296f49b5f265088fb9aa9a))

### Refactoring

- **device summary**: Renamed Doc to Description
  ([`c3dcbfa`](https://github.com/bec-project/bec/commit/c3dcbfa33de54ba6b226afdac212904ffb923f2f))


## v3.15.0 (2025-02-26)

### Features

- **serializer**: Added basic pydantic encoding to serializer
  ([`4804b31`](https://github.com/bec-project/bec/commit/4804b3154bbb01b3510b3585338ca2dad81b92bf))


## v3.14.0 (2025-02-26)

### Documentation

- **async_writer**: Improved docs
  ([`17ec499`](https://github.com/bec-project/bec/commit/17ec499518770715079c247c678a7e74d9952663))

- **redis_connector**: Added an example to raise_alarm
  ([`ebd26af`](https://github.com/bec-project/bec/commit/ebd26afd50830d7b7129de31125cde9e5179c4a3))

### Features

- **async_writer**: Added support for chunked async updates
  ([`1625e2c`](https://github.com/bec-project/bec/commit/1625e2ccee61b34d4b5168b9ba978fb77ad2de5d))


## v3.13.3 (2025-02-26)

### Bug Fixes

- **device**: Fixed used access for properties
  ([`dc7faab`](https://github.com/bec-project/bec/commit/dc7faabff096d7ee849326ed83fec29536e7800b))


## v3.13.2 (2025-02-25)

### Bug Fixes

- **devicemanager**: Fixed type hints for ScanStatusMessage
  ([`ff84cf7`](https://github.com/bec-project/bec/commit/ff84cf7cf37ae17b50e13d0bc0e8e80f59fb6799))


## v3.13.1 (2025-02-24)

### Bug Fixes

- Improve type hinting in redis and endpoints
  ([`7d98dc7`](https://github.com/bec-project/bec/commit/7d98dc70ae6e86f4d7322b2136cdd41f753479b0))

### Refactoring

- Tidy up validate_endpoint
  ([`06ef7cd`](https://github.com/bec-project/bec/commit/06ef7cdc399ef940d9e4d6fd873f01cd20b83f32))


## v3.13.0 (2025-02-24)

### Features

- Add status return for stage/unstage
  ([`f8ae92a`](https://github.com/bec-project/bec/commit/f8ae92a8c8e41fd7cb14b3638c19583642b39d97))

### Refactoring

- Add file_path to ScanStatusMessage, add ScanInfo object to device_manager
  ([`9b57f6b`](https://github.com/bec-project/bec/commit/9b57f6b06708e8a664a2a331a43344ea4164f638))

- Cleanup file_utils compile_file_components and get_full_path
  ([`55ae253`](https://github.com/bec-project/bec/commit/55ae253f1675e8959cb81ee202f21d68b7f7ffbc))

- Cleanup, simplify wait logic on status objects
  ([`c293e98`](https://github.com/bec-project/bec/commit/c293e986b227c5fe01aad03b0f49af1b5e9f67c1))

- **bec_lib**: Cleanup of imports and type checking imports
  ([`03daab5`](https://github.com/bec-project/bec/commit/03daab54ac58a0e59c7452828a073643981f3106))

- **bec_lib**: Moved import of messages to lazy import
  ([`424a956`](https://github.com/bec-project/bec/commit/424a9566c0c1e72214a62ca9eddbf9f5f4af3fd3))

### Testing

- Add checks for invalid names
  ([`87e103d`](https://github.com/bec-project/bec/commit/87e103d03b33b263bdf3a9b8e72324e056c9bc7d))

- Add test for _stage of staged device
  ([`e3835ce`](https://github.com/bec-project/bec/commit/e3835cefd6a6a83482fa5ed175500527fb2f01c5))

- Add test for compile_file_components and get_full_path
  ([`42dedf2`](https://github.com/bec-project/bec/commit/42dedf22a627261e3db6924e9fe40baaa0c0fde9))


## v3.12.0 (2025-02-17)

### Features

- **endpoints**: Include endpoint type in topic path
  ([`ba394e7`](https://github.com/bec-project/bec/commit/ba394e73fb05ff0a608652dea5e5c7f646820ea1))


## v3.11.0 (2025-02-11)

### Bug Fixes

- Add sample name as basic metadata field
  ([`5ef347a`](https://github.com/bec-project/bec/commit/5ef347a96ef6b1f2c0371c3fe5f9528299998b4b))

- Docstrings in metadata schema class and template
  ([`7efdbbf`](https://github.com/bec-project/bec/commit/7efdbbf6d4e8b737bb7da5d26b5e5ba087cd4915))

### Documentation

- Add docs about metadata schema
  ([`2ba5cfc`](https://github.com/bec-project/bec/commit/2ba5cfcca5b6e91fb506e62bdc563e5b838da315))

- Fix typo
  ([`5cf45ae`](https://github.com/bec-project/bec/commit/5cf45aecba77d3b36b956b4c39411cb00a25a3cd))

### Features

- Add ipython magic to print metadata for a scan
  ([`c55eb42`](https://github.com/bec-project/bec/commit/c55eb42c7a036cfadd7667784f2391ca96ec5d07))


## v3.10.3 (2025-02-10)

### Bug Fixes

- **ipython**: Fixed a bug that caused ipython to raise for custom helper functions within the
  device container
  ([`8087ec1`](https://github.com/bec-project/bec/commit/8087ec15861da9015ad95c3e46eab89ea0b0ac6c))


## v3.10.2 (2025-02-05)

### Bug Fixes

- Allow None as default schema
  ([`cd6aa1e`](https://github.com/bec-project/bec/commit/cd6aa1ed117ffcef00585bb92ef9ab90a3bf931a))


## v3.10.1 (2025-02-04)

### Bug Fixes

- Cache metadata reg to reduce warnings
  ([`5d20aff`](https://github.com/bec-project/bec/commit/5d20aff78732040a951d2f3d64ecf1424a855534))


## v3.10.0 (2025-01-30)

### Bug Fixes

- Bugs in create_plugin_structure
  ([`229a90e`](https://github.com/bec-project/bec/commit/229a90e9e2324fe3ded44e909b2019ef567d3939))

- Wrong use of 'any' for 'Any' type
  ([`35fa9a8`](https://github.com/bec-project/bec/commit/35fa9a83efa6e5849aef8d496ce9349115e27c3e))

### Code Style

- Update for black version 25.1.0
  ([`1b1810c`](https://github.com/bec-project/bec/commit/1b1810cccadba189433643ebc4084641ca747208))

### Features

- Allow specifying a default schema per plugin repo
  ([`2efc73b`](https://github.com/bec-project/bec/commit/2efc73b2a8c403dfdf6f74c5313527c060085e16))

- Scan metadata schema plugin
  ([`8bfe544`](https://github.com/bec-project/bec/commit/8bfe54406673de9a6579d1b79550f684d1cee130))

- adds a new submodule to the `scans` module in the plugin repo where models may be defined against
  which the metadata added to ScanQueueMessage should be validated


## v3.9.4 (2025-01-29)

### Bug Fixes

- **raw_message**: Fixed type annotation
  ([`dbd8f1a`](https://github.com/bec-project/bec/commit/dbd8f1a6744b558e8c485d8314fa2617a802c911))

### Continuous Integration

- **formatter**: Install black and isort from bec_lib
  ([`84bb00b`](https://github.com/bec-project/bec/commit/84bb00b5782a6d1ebab859fce4820e308b80fdc7))


## v3.9.3 (2025-01-28)

### Bug Fixes

- **scan_assembler**: Fixed assignment of request inputs for signature containing args
  ([`3acab34`](https://github.com/bec-project/bec/commit/3acab344c4e5c46cb2603beb2a0c036982c8c5ec))


## v3.9.2 (2025-01-27)

### Bug Fixes

- **messages**: Raw message should support any type
  ([`9aa18ee`](https://github.com/bec-project/bec/commit/9aa18eeb2b5104d48793689ef0b7d4436170c2d1))


## v3.9.1 (2025-01-27)

### Bug Fixes

- **scan_data_container**: Get implemented to LazyAttributeDict and LinkedAttributeDict
  ([`f578d5f`](https://github.com/bec-project/bec/commit/f578d5fd810ac053756f0980e3511150ef948892))

### Documentation

- **event_data**: Fixed register example
  ([`1f37e5b`](https://github.com/bec-project/bec/commit/1f37e5ba9d1b6b2b0e54754c6fb8fc789ccc038f))

- **user**: Fixed minor mistake in the example for computed signals
  ([`a0d4642`](https://github.com/bec-project/bec/commit/a0d4642182528b1ccd27ed76bc144a27f634056f))


## v3.9.0 (2025-01-22)

### Features

- **dap**: Added support for custom dap data
  ([`7a0c0be`](https://github.com/bec-project/bec/commit/7a0c0be40dfb15be93d4071d9b140882d29b6f09))

### Testing

- **lmfit**: Adjusted mock data
  ([`c50ea14`](https://github.com/bec-project/bec/commit/c50ea141a8286c531a29d902fef5c89246de1d74))


## v3.8.0 (2025-01-22)

### Features

- Introduce a DAP service for simple statistical analysis on images
  ([`1c225aa`](https://github.com/bec-project/bec/commit/1c225aabfe594a9933037cbd9f73573bc4ad5edf))

### Testing

- Cleanup, refactor and add tests
  ([`c48a97d`](https://github.com/bec-project/bec/commit/c48a97d9bca0f290a8edaea18cec84dfc486d728))


## v3.7.3 (2025-01-21)

### Bug Fixes

- **scan bundler**: Do not run bec emitter without scan id in storage
  ([`98b88a0`](https://github.com/bec-project/bec/commit/98b88a022f617e0c3f8ab28b3410d47c5804a3d7))


## v3.7.2 (2025-01-21)

### Bug Fixes

- Add option to hide table and client info messages; closes #289
  ([`1a3f6e0`](https://github.com/bec-project/bec/commit/1a3f6e0848d54323d5c33e671bd5918ce831afa5))

- Reenable print out of client info messages
  ([`5c8024a`](https://github.com/bec-project/bec/commit/5c8024a9e42b1ee6082a05240c75e3a8fa43eb5d))

### Documentation

- Add docs for client info message and live updates config
  ([`448c88e`](https://github.com/bec-project/bec/commit/448c88ea41b41e8ce856c966a34d52baaadfbc1c))

### Testing

- Add e2e test for client info message
  ([`01b7c2a`](https://github.com/bec-project/bec/commit/01b7c2a3a46016f5f6fe1ebd36e7d7ec57f68496))


## v3.7.1 (2025-01-20)

### Bug Fixes

- **atlas**: Fixed support for streams
  ([`b81cef7`](https://github.com/bec-project/bec/commit/b81cef7fbb6e8bb38b4a88530a1a4d488d6034ef))

- **devicemanager**: Failed devices should not be added
  ([`d95e86b`](https://github.com/bec-project/bec/commit/d95e86bb8f05ee9ad653db1945662987a00531ea))


## v3.7.0 (2025-01-16)

### Documentation

- **messages**: Improved doc string for scan status message
  ([`8a3d647`](https://github.com/bec-project/bec/commit/8a3d64796bdc6ec6beedec44dbe0e92fe3a93d01))

### Features

- **messages**: Flattened scan status message
  ([`380749a`](https://github.com/bec-project/bec/commit/380749a60754cb74c7505e588d4a96380ac0e1f2))

- **scan_status**: Added support for request inputs and scan parameters
  ([`24ff8a2`](https://github.com/bec-project/bec/commit/24ff8a252e537b09a454e44e24aac9b62a25b19c))


## v3.6.0 (2025-01-15)

### Bug Fixes

- **atlas**: Properly shut down atlas connector
  ([`02edeec`](https://github.com/bec-project/bec/commit/02edeec3246b8e101b9dbd753bbef2913db871e9))

- **atlas connector**: Fixed shutdown when disconnected
  ([`204a1a2`](https://github.com/bec-project/bec/commit/204a1a22fc7adcddb3a79df1a79554dbf7a1a626))

- **atlas connector**: Only launch the metada handler and forwarder if we are connected to atlas
  ([`156c99e`](https://github.com/bec-project/bec/commit/156c99e74e529a904c0b0cea337c05bded879d28))

- **redis_connector**: Do not raise for non-BECMessage inputs
  ([`9c828cd`](https://github.com/bec-project/bec/commit/9c828cde3ebe5d410f857ce17de5591c4c3076ad))

- **serialization**: Fixed json decoding for bec messages
  ([`9f47f0b`](https://github.com/bec-project/bec/commit/9f47f0bc7559d5d7429363741f1327cd9c6fdab1))

- **serialization**: Fixed json serializer for BECStatus
  ([`cebb43e`](https://github.com/bec-project/bec/commit/cebb43e72311c4ec4e80351d5e63419f5f4fc12b))

### Build System

- **bec_server**: Removed duplicated entries; removed pyscibec
  ([`4ded7e5`](https://github.com/bec-project/bec/commit/4ded7e5f75f62511cd07820454baec9f701ae436))

### Features

- **atlas**: Added request parser
  ([`4e5eb98`](https://github.com/bec-project/bec/commit/4e5eb98472667caaf324a76538b7c016e7559d67))

- **scihub**: Added bec atlas integration
  ([`bc6f54f`](https://github.com/bec-project/bec/commit/bc6f54fcd96275b4b1fdc004c2be7f7dc6946d14))

- **serialization**: Added json serialization for bec messages
  ([`df7ac75`](https://github.com/bec-project/bec/commit/df7ac75b1d9070bc6ddde344195e069b92d36d49))

### Refactoring

- **atlas connector**: Cleanup and tests
  ([`8be9a54`](https://github.com/bec-project/bec/commit/8be9a545eace7624587b10f7caebfdfdce4c1033))

- **bec_atlas**: Cleanup
  ([`71b3bb0`](https://github.com/bec-project/bec/commit/71b3bb03ef2d0e1373ba55012acb24b720f0818d))


## v3.5.1 (2025-01-14)

### Bug Fixes

- **async_writer**: Fixed bug that caused async writer to overwrite already written datasets
  ([`d63ddb2`](https://github.com/bec-project/bec/commit/d63ddb21173fda86239c05646918d62f14825976))


## v3.5.0 (2025-01-09)

### Bug Fixes

- **scan_status**: Set done status manually if devices are None
  ([`42af11b`](https://github.com/bec-project/bec/commit/42af11b70a5253e73194ed21ad1bb158e194c8fa))

- **scan_stubs**: Fixed check for pending stub status objects
  ([`150210d`](https://github.com/bec-project/bec/commit/150210d41839ee51e7e28a8e82116d68531e1862))

### Features

- **stub_status**: Added name to stub status
  ([`b6f1754`](https://github.com/bec-project/bec/commit/b6f175476c806333eaf5ae0832559bb56994f74c))


## v3.4.3 (2025-01-08)

### Bug Fixes

- **device init**: Improved error handling for disconnected epics motors
  ([`6e7d4c0`](https://github.com/bec-project/bec/commit/6e7d4c0ce48f2d9c8d5f38450d6b4a5068258dac))


## v3.4.2 (2025-01-08)

### Bug Fixes

- **scan_bundler**: Do not run the emitter for messages without scan id
  ([`d15f1da`](https://github.com/bec-project/bec/commit/d15f1dae6aff7051a97dd00ec4ae17c6d2f695c8))

### Continuous Integration

- Disabled dep checks for now
  ([`24f08d5`](https://github.com/bec-project/bec/commit/24f08d5881e6cb5ec2ceee8d7f1b0694ea485ab0))


## v3.4.1 (2025-01-06)

### Bug Fixes

- **device**: Improved misleading error message
  ([`ff475c2`](https://github.com/bec-project/bec/commit/ff475c27585ada58dcb44af9f6cdad8bb5340779))


## v3.4.0 (2024-12-23)

### Bug Fixes

- Simplify code thanks to BEC Widgets MR #422
  ([`899d556`](https://github.com/bec-project/bec/commit/899d556f535f63dd3eb10f71e49b4bf2d55b77f4))

### Features

- Add support for generators in Connector event callbacks
  ([`bf21d29`](https://github.com/bec-project/bec/commit/bf21d29ad79722d7d9141f75012d2f6938775aeb))

Allow to circumvent the problem of blocking calls in callbacks. Be aware that the generator
  execution ("next" and "send") will happen in separate threads, but sequentially.

- New 'GUIAutoUpdateConfigMessage' and corresponding entry point
  ([`8f8b772`](https://github.com/bec-project/bec/commit/8f8b772ae646d48b113e960edd245b70cb719bc9))

This entry point is used to configure auto-update feature from a client.


## v3.3.0 (2024-12-23)

### Features

- New cmdline argument to start redis servers
  ([`8e99f36`](https://github.com/bec-project/bec/commit/8e99f362412f79ed5f47d25859afdac5765ada9f))


## v3.2.0 (2024-12-16)

### Bug Fixes

- **fpdf**: Fixed support for new fpdf interface
  ([`c7b5a92`](https://github.com/bec-project/bec/commit/c7b5a92510f8ad45473f0c5e90f5c8ba71f9fd78))

### Features

- **redis**: Added auth method
  ([`64fcbe3`](https://github.com/bec-project/bec/commit/64fcbe37b91379f5e6717d0ff6d11012722e0f1b))

### Testing

- **dap fit**: Allow for more time to process the dap data
  ([`c2500b4`](https://github.com/bec-project/bec/commit/c2500b472f2f7bb5f9a5df0c8e2e4e414831fdd3))


## v3.1.3 (2024-12-09)

### Bug Fixes

- Bugfix for devicemanager to subscribe to correct event type of ophyd.
  ([`4f8d96a`](https://github.com/bec-project/bec/commit/4f8d96a18c7ce2c6c2ad3f474b6b37ebec6628b4))

### Testing

- Add test for subscriptions to event_types
  ([`25c090c`](https://github.com/bec-project/bec/commit/25c090cd94138fa461d92a26d1856e0f62890175))


## v3.1.2 (2024-12-09)

### Bug Fixes

- Update bec_server dependency for fakeredis
  ([`c8f3b88`](https://github.com/bec-project/bec/commit/c8f3b88ddeda0fb6480369650e889ccb47e16065))

### Build System

- Update pdes, bec_lib for v3, py-scibec>=1.5, pytest-bec-e2e~=3.0, and pydantic~=2.8dep
  ([`e3d4d33`](https://github.com/bec-project/bec/commit/e3d4d33d5790866cf1ae2a2317f9664fd55808a7))


## v3.1.1 (2024-12-06)

### Bug Fixes

- Bugfix to capture exception for console_log
  ([`9c3c088`](https://github.com/bec-project/bec/commit/9c3c0884d1d8458c0cc93119a8d6a0a26c23dc1e))


## v3.1.0 (2024-11-27)

### Continuous Integration

- Fix ci syntax for check package job
  ([`68a6b5b`](https://github.com/bec-project/bec/commit/68a6b5b41db83d4ae85ddfad41a4220328c29844))

### Features

- Use callback to asynchronously setup GUI at startup, use proxy to defer GUI commands
  ([`d6d7e86`](https://github.com/bec-project/bec/commit/d6d7e862aef1436b38acf3b4c2c3cb7208625ab6))


## v3.0.0 (2024-11-18)

### Bug Fixes

- Bugfix in PipelineMock; _pipe_buffer and _connector become istance variables
  ([`eefe1e3`](https://github.com/bec-project/bec/commit/eefe1e397191ddb91a9b563cbdca76107f9f525d))

- Fix bug related to usage of ConnectorMock
  ([`1978ee4`](https://github.com/bec-project/bec/commit/1978ee4f30fff43696819f2dcdd3f0936ec39960))

- **async_writer**: Flush to disk after writing to make the data available to readers
  ([`012832b`](https://github.com/bec-project/bec/commit/012832ba324ba250f3f1b5b7d91c9514df319178))

- **bec_client**: Added scan_def to scans namespace
  ([`7c854ef`](https://github.com/bec-project/bec/commit/7c854efda0578061774963396ee8b2bff92daa53))

- **cont_line_scan**: Wait for trigger before moving on
  ([`07edd47`](https://github.com/bec-project/bec/commit/07edd47821b2df8f8a9ac19ee322bf10c1ad6900))

- **enpoints**: Allow delete on streams
  ([`fbbc57a`](https://github.com/bec-project/bec/commit/fbbc57a0969256ce2b506102505f717112e94263))

- **fermat_spiral**: Scan motors should use the defined motors, not args
  ([`9c895e4`](https://github.com/bec-project/bec/commit/9c895e47b1ccb8ca3fdd196f4d54037cc888d332))

- **file_writer**: Fixed support for mixed data types
  ([`d7cf798`](https://github.com/bec-project/bec/commit/d7cf798034a6ccaa794329136a4dc4bc8a9bfdc8))

- **scan**: Fixed stub guard
  ([`a980e9e`](https://github.com/bec-project/bec/commit/a980e9e885259c22310d8a9112188ab929f0bc43))

- **scan guard**: Only check strings
  ([`e2583ed`](https://github.com/bec-project/bec/commit/e2583edb5af4aa495b541fa82a4f3fe5557653bc))

- **scan stubs**: Fixed support for receiving results from multiple stubs; closes #375; cleanup of
  docs
  ([`ddf2faf`](https://github.com/bec-project/bec/commit/ddf2faf148413abe4862fd06807827f1644d85c4))

- **scan_data_container**: Fixed read access
  ([`3e68e80`](https://github.com/bec-project/bec/commit/3e68e80725e75b6e9d8b9393c9987d12960c278a))

- **scan_history**: Fixed cleanup routine
  ([`9eaf398`](https://github.com/bec-project/bec/commit/9eaf398c36814bf5ecae52025d560a2c3f47de7f))

- **scan_stub**: Do not wait for the status result
  ([`f9e17fc`](https://github.com/bec-project/bec/commit/f9e17fc6bb16cc79b7f396dc44e4ef440ecc768e))

- **scan_stubs**: Done check must also propagate to sub status objects
  ([`e0822ba`](https://github.com/bec-project/bec/commit/e0822ba034672073c71d28990f651ce58255adcd))

- **scans**: Fixed various smaller bugs and improved the doc strings
  ([`7d50839`](https://github.com/bec-project/bec/commit/7d50839179b75b668a0ba9a29ec8c72e049e6240))

- **scans**: Scan id should back-propagate to the scan instance
  ([`a6c8d18`](https://github.com/bec-project/bec/commit/a6c8d18487d4da0c8695123d664591ad12f06916))

### Continuous Integration

- Added missing e2e run for pre-release
  ([`14c1b49`](https://github.com/bec-project/bec/commit/14c1b49c4261519a38f2426680e86caecd8a3463))

### Documentation

- Added reference to test docs to step scan tutorial
  ([`1063bc7`](https://github.com/bec-project/bec/commit/1063bc72a841f472e1b48b4f09db7861b776124f))

- Scan docs improvements
  ([`55b2107`](https://github.com/bec-project/bec/commit/55b2107f27dce668a27a8aeb9d0e52cdcfbc8bd6))

- **data access**: Cleanup
  ([`fcbdf91`](https://github.com/bec-project/bec/commit/fcbdf911fcd388757adea0adee3d65369c7e7a9e))

- **data access**: Fixed typo
  ([`12a041f`](https://github.com/bec-project/bec/commit/12a041fdaea6c23291400e72cb70bf357a74a84f))

- **data_access**: Updated docs for new data access pattern
  ([`454284e`](https://github.com/bec-project/bec/commit/454284e8c8f9650675d7f2fb0192a117bfd11fc8))

- **scan tutorial**: Updated tutorial with new stubs interface
  ([`df2ca84`](https://github.com/bec-project/bec/commit/df2ca846866a27b8cc7c7be8b4c1aac508788702))

- **scan_server**: Cleanup
  ([`ebf1280`](https://github.com/bec-project/bec/commit/ebf12802e7cd98371264c6ac80439938b7121cb9))

- **scans**: Cleanup
  ([`1ddd4de`](https://github.com/bec-project/bec/commit/1ddd4de4556101ba24e5628b92533796f7793ed0))

- **tutorial**: Extended explanation for fixtures
  ([`106ce80`](https://github.com/bec-project/bec/commit/106ce80aed519734157834cce7ba7e24312ca824))

### Features

- Add file_event as new SUB_EVENT to device manager; closes #335
  ([`ccbde45`](https://github.com/bec-project/bec/commit/ccbde45a48a19fb2a7580bc9f332b424ac06e41d))

- **data_access**: Moved to file access
  ([`1d8bc6d`](https://github.com/bec-project/bec/commit/1d8bc6d7ec33261fab8835b2fccafa6ac3a8805a))

BREAKING CHANGE: baseline data and async data is no longer cached on the client and instead provided
  through file references. To this end, the data access through scan_item.data has been generalized.
  Live data (i.e. monitored data) is now accessible through scan_item.live_data

- **file_writer**: Added async file writer
  ([`f787e6d`](https://github.com/bec-project/bec/commit/f787e6d23d97d64a40ad2323930bd1c3800f0a39))

- **scan**: Added stub registry
  ([`0c934c1`](https://github.com/bec-project/bec/commit/0c934c161975b7111432ffa9442b653c0cc7fe96))

- **scan_history**: New file-based scan history
  ([`5f4f12b`](https://github.com/bec-project/bec/commit/5f4f12b6f027f0c1bcacc7e0fadec741ff27dc02))

- **scan_server**: Moved to unique device instruction ids and ScanStubStatus objects
  ([`a4f99e2`](https://github.com/bec-project/bec/commit/a4f99e29f9199888167944fc27be0b143f1bf8e0))

BREAKING CHANGE: This commit changes the scan definition to use unique device instruction ids and
  ScanStubStatus objects. This is a breaking change and will require updates to the scan
  definitions.

- **scans**: Added return_to_start kwarg to all scans. If set to true, the scan returns to start.
  ([`1032dd5`](https://github.com/bec-project/bec/commit/1032dd5856a2d5ad41d3eb3b17035a4f5f6c6bbf))

BREAKING CHANGE: In the past, this was always set to true. Now, the default will depend on the
  relative flag. Relative scans return to start to avoid shifts for repeated scans. Absolute scans
  will not return to start. However, this behaviour can be overridden by providing the additional
  return_to_start kwarg or subclassing the scan base.

closes #332

- **stubs**: Added support for setting multiple devices at once
  ([`41683ae`](https://github.com/bec-project/bec/commit/41683ae5f86fa675ead4c559324eef0962c3ddf7))

### Refactoring

- **async_writer**: Cleanup
  ([`11e356e`](https://github.com/bec-project/bec/commit/11e356e90bf696e3fd23c8edd5d043482c842982))

- **bec_lib**: Minor improvements to type hints
  ([`f6ae344`](https://github.com/bec-project/bec/commit/f6ae344b3408baba99b4f3c9847e8ab428ab5196))

- **bec_lib**: Renamed ScanData to LiveScanData
  ([`b77211f`](https://github.com/bec-project/bec/commit/b77211fe869438c4453f4be06205a6aae6993355))

- **bec_service**: Minor type hints improvements
  ([`15ce6cf`](https://github.com/bec-project/bec/commit/15ce6cfb97ca620b534f971430d92fcce996a797))

- **endpoints**: Allow for type checks despite lazy import
  ([`710bd8a`](https://github.com/bec-project/bec/commit/710bd8ab82cbae8a175cd787db0ae64fc12c3ffb))

- **file_writer**: Cleanup
  ([`a5a860b`](https://github.com/bec-project/bec/commit/a5a860bd3b662db27d0f0b1b4722a70a4bdd5234))

- **scan stubs**: Added typecheck
  ([`981acce`](https://github.com/bec-project/bec/commit/981acceda032af0ad2ca272afdcb03ee1a8b74cd))

- **scan_server**: Minor cleanup
  ([`c425909`](https://github.com/bec-project/bec/commit/c425909e221efc25da20a9957f81bf63c2e587b1))

- **scan_stubs**: Changed default to wait=True
  ([`9114a58`](https://github.com/bec-project/bec/commit/9114a58875197a2f6410e26b613ad87bbc672ac8))

- **scans**: Fixed pylint pragma
  ([`1c1cc21`](https://github.com/bec-project/bec/commit/1c1cc2135bc888429dbb9ad32b2c74191f5de3fd))

- **scans**: Moved fixtures to scan server module
  ([`d2a738b`](https://github.com/bec-project/bec/commit/d2a738b8c26c6434f8fcfce1b83cb1f766b589c3))

- **scans**: Renamed _get_scan_motors to update_scan_motors; minor fixes
  ([`da43279`](https://github.com/bec-project/bec/commit/da432792fd3b35df71e60686deab44736728d517))

- **scans**: Renamed group primary to group monitored
  ([`7b03fc3`](https://github.com/bec-project/bec/commit/7b03fc36b6833302475a0f2a5172cd7aa967a3b3))

- **stubs**: Renamed rpc to send_rpc
  ([`49cd06e`](https://github.com/bec-project/bec/commit/49cd06e60ef470d5c3a2b5534bf0e6fac1dd95b2))

### Testing

- Added test to ensure access to status results is not blocking
  ([`06ad81f`](https://github.com/bec-project/bec/commit/06ad81f1d106b01e0c3052e92d3912c0dde0d1cb))

- **scan_history,data_container**: Added tests; refactored to use conftest for shared fixtures
  ([`ec57e07`](https://github.com/bec-project/bec/commit/ec57e077a2cfbee0e6344427302d49634c1939b2))

### Breaking Changes

- **scans**: In the past, this was always set to true. Now, the default will depend on the relative
  flag. Relative scans return to start to avoid shifts for repeated scans. Absolute scans will not
  return to start. However, this behaviour can be overridden by providing the additional
  return_to_start kwarg or subclassing the scan base.


## v2.34.15 (2024-11-11)

### Bug Fixes

- **client**: Fixed queue item update
  ([`243af03`](https://github.com/bec-project/bec/commit/243af03f8cdc02ca487fa547c4ccca2659590313))


## v2.34.14 (2024-11-11)

### Bug Fixes

- Issue #346: activate matplotlib IPython's hook only when needed
  ([`a9727aa`](https://github.com/bec-project/bec/commit/a9727aaeb0ea225a8c68877d7e7ad8ac21ad44c1))


## v2.34.13 (2024-11-05)

### Bug Fixes

- Fix permission issue for file created by other user
  ([`f41e8bc`](https://github.com/bec-project/bec/commit/f41e8bcabc5c9c1f705cfaa985f91cd0b03292bf))


## v2.34.12 (2024-10-22)

### Bug Fixes

- **scan_assembler**: Forward error message content
  ([`6fb87c7`](https://github.com/bec-project/bec/commit/6fb87c725bdb0cca93c880a7869aac06e3851dd7))

- **scans**: Fixed kwarg check for scans without args but required kwargs
  ([`96734c3`](https://github.com/bec-project/bec/commit/96734c3dd2ec08e539081f282669af7b95571c10))

- **scans**: Removed default from required kwarg
  ([`5c0d825`](https://github.com/bec-project/bec/commit/5c0d8257858adc7b282d97964b88150faffd33a7))


## v2.34.11 (2024-10-22)

### Bug Fixes

- Ensure 'bec.scans' (default namespace) points to a clean namespace
  ([`e51a2fb`](https://github.com/bec-project/bec/commit/e51a2fbe1c956405b1e8dc0f66e3e76bf346af70))

BECClient.scans is the same as before ; BECClient.scans_namespace is a SimpleNamespace with only
  functions to run scans. BECClient's default namespace points to the latter. As a consequence,
  IPython client "bec.scans" == BECClient.scans_namespace , users can only see scan run functions.


## v2.34.10 (2024-10-22)

### Bug Fixes

- **scans**: Expose snaked kwarg to user
  ([`121b9eb`](https://github.com/bec-project/bec/commit/121b9eb57ad022d794e8bf79a7f2d10e6761ce46))

### Continuous Integration

- Run e2e tests on pre_release
  ([`e35477b`](https://github.com/bec-project/bec/commit/e35477b3d69d6a8bc3a0ef384a91397841c3a493))


## v2.34.9 (2024-10-18)

### Bug Fixes

- **rpc**: Reset alarm stack for rpc
  ([`13d6ec4`](https://github.com/bec-project/bec/commit/13d6ec472b15f34847175e6811a43f6329739f79))


## v2.34.8 (2024-10-15)

### Bug Fixes

- Fixed folder permission for log and recovery_config writing
  ([`d2ce95c`](https://github.com/bec-project/bec/commit/d2ce95c10eab96d8358259039fe1349ae6a780ad))


## v2.34.7 (2024-10-11)

### Bug Fixes

- **log**: Fixed file permissions
  ([`29af160`](https://github.com/bec-project/bec/commit/29af160ab9de5a699f81db319d6d0d92c2a51ec0))

### Testing

- **file_utils**: Fixed tests
  ([`9b30f00`](https://github.com/bec-project/bec/commit/9b30f004ac4b86637798eba3089f9dea9cb70e92))


## v2.34.6 (2024-10-11)

### Bug Fixes

- **logger**: Log messages should be streams, not pub sub
  ([`9141280`](https://github.com/bec-project/bec/commit/914128088986e0473d9a74c56e3c4d17e60dc9ee))


## v2.34.5 (2024-10-11)

### Bug Fixes

- **logs**: Adjusted mask for log directory
  ([`975e047`](https://github.com/bec-project/bec/commit/975e0472572697c69529b100f65d0c0820221532))

### Continuous Integration

- Added CHECK_PKG_VERSIONS variable to ci file
  ([`5196e8e`](https://github.com/bec-project/bec/commit/5196e8eacaf138203e2c9b044e520bab3015da35))

### Documentation

- Update outdated yaml for simulated gauss in user section
  ([`eb84c67`](https://github.com/bec-project/bec/commit/eb84c673246044e92dcbcd2e041ff7aa0e14ff2a))

- **scans**: Added missing relative flag to umv and mv examples
  ([`6231724`](https://github.com/bec-project/bec/commit/62317242ef4f6de3fa9f8d4fb10e9c3020a78253))


## v2.34.4 (2024-10-02)

### Bug Fixes

- Add 1d monitor endpoint, add waveform device and fix its updates. unify async 'extend' and
  'append' accumulation
  ([`a06d03f`](https://github.com/bec-project/bec/commit/a06d03f212169e0a8104827c6ed50171abd42341))

### Documentation

- Update documentation
  ([`f836910`](https://github.com/bec-project/bec/commit/f836910b3d58f9207bdf388956ddf2c7e4d52a43))


## v2.34.3 (2024-10-01)

### Bug Fixes

- Fixed support for nested devices
  ([`56e98e8`](https://github.com/bec-project/bec/commit/56e98e8b19b6698cc96392429ef4922b20ec38bb))


## v2.34.2 (2024-10-01)

### Bug Fixes

- Fixed min version of msgpack
  ([`00d9572`](https://github.com/bec-project/bec/commit/00d957267a70bbe56f25a36f8b8ae9d7dcbd97d6))


## v2.34.1 (2024-10-01)

### Bug Fixes

- Min version for typeguard to support literals is 4.1.5
  ([`c2e05c0`](https://github.com/bec-project/bec/commit/c2e05c04febf8ef019f6c08fdf890627158bb5a4))


## v2.34.0 (2024-10-01)

### Build System

- Updated numpy and hiredis version
  ([`88f5e9d`](https://github.com/bec-project/bec/commit/88f5e9d508d924d7e364808b2a8476a72bb365a4))

### Features

- **utils**: Added plugin repo license
  ([`d32ed2b`](https://github.com/bec-project/bec/commit/d32ed2b6deb4bbb6fbfb5833ad5c341249967b39))

### Refactoring

- Allow rgb array data within DeviceMonitor2DMessage
  ([`eb3c302`](https://github.com/bec-project/bec/commit/eb3c30287410f76eadacadb3e53f98e6d44c4406))


## v2.33.0 (2024-09-18)

### Bug Fixes

- **interactive_scan**: Fixed bug in scan number calculation; simplified interface
  ([`ca4eb1e`](https://github.com/bec-project/bec/commit/ca4eb1e5d42b272f5de4620ac73d5ae415136415))

- **interactive_scan**: Fixed default exp_time
  ([`e9839bf`](https://github.com/bec-project/bec/commit/e9839bf79b44a004b1d3050d8cf3d71c86fff5e4))

- **rpc**: Check for alarms during rpc calls
  ([`99a1553`](https://github.com/bec-project/bec/commit/99a1553794766ebbe9441a16b29a91be5dfba162))

- **scan_worker**: Exp time is optional; default 0
  ([`90b07ed`](https://github.com/bec-project/bec/commit/90b07ed690da05e397bc6971d497b09d583e0ac4))

### Features

- **interactive scans**: Added support for interactive scans
  ([`d842e20`](https://github.com/bec-project/bec/commit/d842e20dc8df809413a4017539019d8d644bcf68))

This commit adds support for interactive scans. This is a new feature that allows users to
  interactively define scans in the BEC client. The user can specify the scan parameters in a
  context manager and then run the scan interactively by accessing the device objects directly.
  Dedicated trigger and read function are provided to perform larger actions.

### Refactoring

- **bec_lib**: Minor cleanup for cli scan context managers
  ([`c67e80a`](https://github.com/bec-project/bec/commit/c67e80a29a1aa50566192f5f45d2e895256cfc40))

### Testing

- **dmmock**: Fixed mocked devicemanager to avoid leakage between tests
  ([`020856d`](https://github.com/bec-project/bec/commit/020856d4febba1fa24e073fe27e6165b8ccdd450))


## v2.32.0 (2024-09-18)

### Features

- **endpoint**: Added device_raw endpoint
  ([`1a9bb96`](https://github.com/bec-project/bec/commit/1a9bb962f28f8f0c4319c9ee3177a6ca8cc660d1))

- **scan queue**: Added support for changing the order of scans in the queue
  ([`7eb4ead`](https://github.com/bec-project/bec/commit/7eb4ead57d163e045a88f3af906aa928cc2bca0f))


## v2.31.2 (2024-09-13)

### Bug Fixes

- **dap**: Dap service should run independent of scan segment callbacks
  ([`026b12f`](https://github.com/bec-project/bec/commit/026b12f2025ea33fd0be52518345834198364119))


## v2.31.1 (2024-09-12)

### Bug Fixes

- Get "egu" (engineering units) from device from configuration, not through RPC call
  ([`7331d3c`](https://github.com/bec-project/bec/commit/7331d3c4171465fa89597450a1c17beeaac2dc38))

### Continuous Integration

- Fetch all tags
  ([`77be5e1`](https://github.com/bec-project/bec/commit/77be5e1369d31d96cfee2a3fecf049dc1a1b70dc))

- Unshallow fetch to retrieve all commits for the changelog
  ([`395ccee`](https://github.com/bec-project/bec/commit/395ccee57053e8bd55e31e8d2eab27478fa99d88))


## v2.31.0 (2024-09-05)

### Features

- **scan_report**: Added public files to scan item and report on the master file in scan report
  ([`adca248`](https://github.com/bec-project/bec/commit/adca248dfd592b1cdbfa2cddf1a10c13ac11e176))

### Testing

- Added test for file events
  ([`b976fb4`](https://github.com/bec-project/bec/commit/b976fb4a084910da5ae0a4fac32d1152b0f1cc04))


## v2.30.2 (2024-09-05)

### Bug Fixes

- Updated device_config of pseudo_signal1
  ([`529663f`](https://github.com/bec-project/bec/commit/529663f4fdef795c7863622ee01328bf3a1385a6))

### Refactoring

- Refactoring to make scan_manager optional kwarg
  ([`a5ccefa`](https://github.com/bec-project/bec/commit/a5ccefa978aa2b3c79d7b4617614bbde689036a0))

- Scanitem attributes bec and callback made private
  ([`a70af8f`](https://github.com/bec-project/bec/commit/a70af8f58cba31294a6b84bb1b45d62f2dcb4cc0))

### Testing

- Fix tests
  ([`46738ad`](https://github.com/bec-project/bec/commit/46738ad259c9e99095067b67ffbacbaff83115ea))


## v2.30.1 (2024-09-05)

### Bug Fixes

- Bugfix in cont_line_scan; reworked device and signal mocks
  ([`c91dcf4`](https://github.com/bec-project/bec/commit/c91dcf4d37bc1add18d2f0682af97358e4abdee6))

- Fix hints for devices of type ophyd.signal
  ([`1b8b2c7`](https://github.com/bec-project/bec/commit/1b8b2c7b490113e0b7acd3a070c9bec1c1626b4f))

### Refactoring

- Reworked R/W info in device info; removed bug for devices with type Signal
  ([`d0ee4ec`](https://github.com/bec-project/bec/commit/d0ee4ec5544dcc400568dc5311cea0e1d4074c8e))


## v2.30.0 (2024-09-04)

### Features

- **logger**: Added option to disable modules; added retention and rotation; changed log format for
  stderr
  ([`868f40d`](https://github.com/bec-project/bec/commit/868f40db8e1420dab7eaf3fed6eed2e8313ab539))


## v2.29.0 (2024-09-02)

### Bug Fixes

- **device_manager**: Fixed init value for failed devices
  ([`61c4fb6`](https://github.com/bec-project/bec/commit/61c4fb69cdc068bdc997a53b26fccc15f00217b1))

### Continuous Integration

- Prefill variables for manual pipeline start
  ([`d4b4bf8`](https://github.com/bec-project/bec/commit/d4b4bf816a73923a90d0e7d1d5158f0e26016e92))

### Features

- **config**: Added support for adding and removing devices
  ([`070b041`](https://github.com/bec-project/bec/commit/070b0417d80c56b69093c768d25238cb0465de36))


## v2.28.0 (2024-09-02)

### Features

- **queue schedule**: Added endpoint and queue schedule methods
  ([`0c7e0eb`](https://github.com/bec-project/bec/commit/0c7e0eb37f3d88e94bbb0ae0ee346b9736bc582c))


## v2.27.0 (2024-08-30)

### Bug Fixes

- **ipython client**: Fixed magic command for resume
  ([`2289036`](https://github.com/bec-project/bec/commit/228903628b3dd624a20bea48ccf65ec9ff1cc5ed))

- **queue**: Moved queue modifications to dedicated message for the device server
  ([`3e0e5cf`](https://github.com/bec-project/bec/commit/3e0e5cf9a8ab477acdbeb85b703beb86207fec18))

### Documentation

- **stubs**: Improvements to the stubs doc strings
  ([`89b4353`](https://github.com/bec-project/bec/commit/89b4353433c603398e8c87da36e6ebc7aa2fc47c))

- **stubs**: Minor improvements to the wait docstring
  ([`9db0c03`](https://github.com/bec-project/bec/commit/9db0c03bec9aa2fa50e2ad727d0a43727c2db482))

### Features

- **endpoint**: Added stop_all_devices endpoint
  ([`13beb51`](https://github.com/bec-project/bec/commit/13beb51a520e9ef6569fff45807bd50d076ce787))

### Refactoring

- **docs**: New bec logo
  ([`4070521`](https://github.com/bec-project/bec/commit/4070521e6c4b6b8ee6b29730fdefb5def2f5be22))


## v2.26.0 (2024-08-22)

### Features

- **bec_lib**: Print all asap client messages during rpc
  ([`5de3235`](https://github.com/bec-project/bec/commit/5de3235788f5bc573e2b1daa2c81c977e200e921))


## v2.25.1 (2024-08-22)

### Bug Fixes

- Try/expect CONSOLE logger changed order
  ([`ca36128`](https://github.com/bec-project/bec/commit/ca3612816bcb1bd86bc2480724fad57ce9af9892))


## v2.25.0 (2024-08-22)

### Features

- **server**: Added endpoint and handler to restart server through redis
  ([`9bde681`](https://github.com/bec-project/bec/commit/9bde68138c5930c0f050ffd9ee6fcd21a294a488))


## v2.24.0 (2024-08-21)

### Features

- **lmfit**: Added fallback to hinted signals; added oversampling option
  ([`b66b928`](https://github.com/bec-project/bec/commit/b66b9286899a69ab8bc71ec2a65e16189e52cb07))


## v2.23.2 (2024-08-21)

### Bug Fixes

- **docs**: Scan gui config tutorial added to toc
  ([`343309f`](https://github.com/bec-project/bec/commit/343309ff5e224227e15076fc94a124a4c76262b4))


## v2.23.1 (2024-08-19)

### Bug Fixes

- **serialization**: Added json decoder as fallback option for raw messages
  ([`5e7f630`](https://github.com/bec-project/bec/commit/5e7f630ce7b2e7a3ff3337d966155e4b5f5cc7ff))

### Testing

- Wait for dap to finish
  ([`be0d589`](https://github.com/bec-project/bec/commit/be0d589ae89cc663687402fd4c2fb0a738643f22))


## v2.23.0 (2024-08-17)

### Features

- **client**: Added client event for updated devices
  ([`7573ce1`](https://github.com/bec-project/bec/commit/7573ce1b52e47106dfa7ab8b814420aeb1d14591))


## v2.22.1 (2024-08-16)

### Bug Fixes

- Remove unused imports, add missing import
  ([`92b5e4a`](https://github.com/bec-project/bec/commit/92b5e4a50b45ee9d960fcf9839500fc420b9e0be))

### Testing

- Add connector unregister test with 'patterns'
  ([`7f93933`](https://github.com/bec-project/bec/commit/7f93933847dd387847930fb81171ca29f1b2d3be))


## v2.22.0 (2024-08-16)

### Bug Fixes

- Fixed bug in client fixture for loading configs
  ([`7636f4d`](https://github.com/bec-project/bec/commit/7636f4d15a36a4f32a202643771e4b5d97ff5ae6))

- **client**: Handle deviceconfigerrors more gracefully in the console
  ([`433b831`](https://github.com/bec-project/bec/commit/433b8313021eb89fd7135fa79504ba34270d12eb))

### Continuous Integration

- Install ophyd_devices from the repo
  ([`1e805b4`](https://github.com/bec-project/bec/commit/1e805b47c6df2bc08966ffd250ba0b3f22ab9563))

- Use target branch instead of default pipeline branch for e2e tests
  ([`83e0097`](https://github.com/bec-project/bec/commit/83e00970d1e5f105ee3e05bce6fd7376bd9698e4))

### Documentation

- Update dev docs
  ([`82ffc52`](https://github.com/bec-project/bec/commit/82ffc521760fda34c594f89f10c174ae0b959710))

renamed bec_config to bec_service_config; removed pmodule instructions as they are not available
  anymore

### Features

- **device_server**: Gracefully handle timeouts
  ([`ec5abd6`](https://github.com/bec-project/bec/commit/ec5abd6dde4c71e41395ee6f532f27f24215e168))

Failed config updates should only lead to config flush if the object initialization fails. If we
  simply can't connect to the signals, the device should be disabled.

### Testing

- Ensure BECClient singleton is reset
  ([`75dd67b`](https://github.com/bec-project/bec/commit/75dd67ba17ab0d79881501f2f902ef0a8c2233a2))

- Fixed data access in dummy controller device
  ([`624c257`](https://github.com/bec-project/bec/commit/624c25763fdef2a9ee913e5936311f421bd9b8d6))

- Use simpositionerwithcontroller for controller access
  ([`49b53a9`](https://github.com/bec-project/bec/commit/49b53a95d9317c6ec1bf14c81e2b3886788690d5))


## v2.21.5 (2024-08-14)

### Bug Fixes

- **tmux**: Retry tmux launch on error
  ([`8ba44f6`](https://github.com/bec-project/bec/commit/8ba44f6eef7bd9f118933ba03900134d9bb6cf32))

Sometimes, restarting the tmux client is flaky


## v2.21.4 (2024-08-14)

### Bug Fixes

- **client**: Fixed client init of singleton instance
  ([`cfae861`](https://github.com/bec-project/bec/commit/cfae8617fdb7f7a7fc613206f0f27d7274d899c1))


## v2.21.3 (2024-08-13)

### Bug Fixes

- Fix bug in bluesky emitter get descriptor method
  ([`27fa758`](https://github.com/bec-project/bec/commit/27fa7584cd61c6453db01ab05f49b9c712155641))


## v2.21.2 (2024-08-13)

### Bug Fixes

- **bec_lib**: Raise on rpc status failure
  ([`efc07ff`](https://github.com/bec-project/bec/commit/efc07ff4ff6ddf810d3a40ec52b35877e7ae67a7))

### Testing

- Fixed test for status wait
  ([`4c5dd4a`](https://github.com/bec-project/bec/commit/4c5dd4ab40a0c8d2ebef38d36ec61c230243f649))


## v2.21.1 (2024-08-13)

### Bug Fixes

- **bec_lib**: Added check to ensure becmessage type is correct
  ([`c8b4ab9`](https://github.com/bec-project/bec/commit/c8b4ab9d99530351fa2005b69e118a5fb563d1e3))

- **bec_lib**: Fixed reported msg type for device_config endpoint
  ([`28f9882`](https://github.com/bec-project/bec/commit/28f98822173cba43860dcd20f890fee93a978d6a))

- **redis_connector**: Fixed support for bundle message
  ([`ef637c0`](https://github.com/bec-project/bec/commit/ef637c0e59f94ad471ec1dce5906a56ae0299f9a))

### Refactoring

- Minor cleanup
  ([`f08c652`](https://github.com/bec-project/bec/commit/f08c652dd6eca114331be4b915bec66fe911ff12))

- **scan_bundler**: Moved specific bec emitter methods from emitterbase to bec emitter
  ([`b0bc0da`](https://github.com/bec-project/bec/commit/b0bc0da54f66e5ad4d26471c88eb7d1c8910bead))


## v2.21.0 (2024-08-13)

### Documentation

- **messaging**: Added first draft of bec messaging docs
  ([`efbeca3`](https://github.com/bec-project/bec/commit/efbeca3c322fa62a95b51ebc5670a6d446dcdebc))

### Features

- Add metadata entry to _info for signal and device
  ([`fe4979a`](https://github.com/bec-project/bec/commit/fe4979adbd4804c6f3b69902ade0d22c1b70f8cd))

### Testing

- Fix tests for adapted device_info
  ([`8778843`](https://github.com/bec-project/bec/commit/877884336b52aa9e66e8b463fcb3bc7abcd654d1))


## v2.20.2 (2024-08-01)

### Bug Fixes

- Do not import cli.launch.main in __init__
  ([`45b3263`](https://github.com/bec-project/bec/commit/45b32632181fff18758e2195b84f8254f365465a))

This has the side effect of reconfiguring loggers to the level specified in the main module (INFO in
  general)

### Continuous Integration

- Added support for child pipelines
  ([`d3385f6`](https://github.com/bec-project/bec/commit/d3385f66e50e6b19e79030ec0af13054a7ab2f47))

- Made jobs interruptible
  ([`1fc6bc4`](https://github.com/bec-project/bec/commit/1fc6bc4b22c48715eff4d27709cffc5c08037769))


## v2.20.1 (2024-07-25)

### Bug Fixes

- Unpack args and kwargs in scaninfo
  ([`2955a85`](https://github.com/bec-project/bec/commit/2955a855ca742e4cafcf33cc262b439c5afb2b5e))

### Continuous Integration

- Added child_pipeline_branch var
  ([`8ca8478`](https://github.com/bec-project/bec/commit/8ca8478019b532db2ab2f5c0fbc8297ca9d56327))

- Added inputs to beamline trigger pipelines
  ([`5e11c0c`](https://github.com/bec-project/bec/commit/5e11c0c06543a5d6f875575fe2a3cf9748421c5d))

- Cleanup and moved beamline trigger pipelines to awi utils
  ([`3030451`](https://github.com/bec-project/bec/commit/303045198ec77c7a6b7ef5d5e7c4ab308c14a52f))

- Wip - downstream pipeline args for ophyd
  ([`81b1682`](https://github.com/bec-project/bec/commit/81b168299bf9f05085b61eafe94aa3bc279c41b4))

- Wip - downstream pipeline args for ophyd
  ([`a5712c3`](https://github.com/bec-project/bec/commit/a5712c379da39861b69bbb9129ea91eac6bbfda0))

### Testing

- Fix msg in init scan info
  ([`1357b21`](https://github.com/bec-project/bec/commit/1357b216a83d130efb3ba9af21c0a1eef7d3a9e1))


## v2.20.0 (2024-07-25)

### Build System

- **ci**: Pass ophyd_devices branch to child pipeline
  ([`a3e2b2e`](https://github.com/bec-project/bec/commit/a3e2b2e37634fe7f445cce7e0ff2ac0b01d093b3))

### Features

- Add device_monitor plugin for client
  ([`c9a6f3b`](https://github.com/bec-project/bec/commit/c9a6f3b1fad8cbb455c6a79379e03efa73fe984d))

### Refactoring

- Renamed device_monitor to device_monitor_2d, adapted SUB_EVENT name
  ([`c7b59b5`](https://github.com/bec-project/bec/commit/c7b59b59c16ac18134ab73bf020137d28da56775))

- Renamed DeviceMonitor2DMessage
  ([`0bb42d0`](https://github.com/bec-project/bec/commit/0bb42d01bf7d7a03cf8e2a0859582ab14d8c99b8))


## v2.19.1 (2024-07-25)

### Bug Fixes

- Add velocity vs exp_time check for contline_scan to make it more robust
  ([`2848682`](https://github.com/bec-project/bec/commit/2848682644624c024ac37fe946fbd2b6ddc377dc))


## v2.19.0 (2024-07-19)

### Bug Fixes

- Make a CONSOLE_LOG level to be able to filter console log messages and fix extra line feed
  ([`7f73606`](https://github.com/bec-project/bec/commit/7f73606dfc4b4b97afe1f85a641626f0ab134b34))

- Prevent already configured logger to be re-configured
  ([`dfdc397`](https://github.com/bec-project/bec/commit/dfdc39776e1cadffc53cf0193d2fa1791df821d5))

### Features

- Add "parse_cmdline_args" to bec_service, to handle common arguments parsing in services
  ([`41b8005`](https://github.com/bec-project/bec/commit/41b80058f8409131be483950dfb88e7b93282bff))

Add "--log-level" and "--file-log-level" to be able to change log level from the command line

### Refactoring

- Use 'parse_cmdline_args' in servers
  ([`06902f7`](https://github.com/bec-project/bec/commit/06902f78240c5ded0674349a125fd80f30aab580))


## v2.18.3 (2024-07-08)

### Bug Fixes

- **bec_lib**: Fixed bug that caused the specified service config to be overwritten by defaults
  ([`5cf162c`](https://github.com/bec-project/bec/commit/5cf162c19d573afde19f795a968f1513461aec9a))


## v2.18.2 (2024-07-08)

### Bug Fixes

- **bec_lib**: Accept config as input to ServiceConfig
  ([`86714ae`](https://github.com/bec-project/bec/commit/86714ae57b5952eaa739a5ba60d20aa6ab51bf91))

### Testing

- Fixed test for triggered devices
  ([`05e82ef`](https://github.com/bec-project/bec/commit/05e82efe088a9ad0ac24542099c1008562287dbf))


## v2.18.1 (2024-07-04)

### Bug Fixes

- Add async monitor to config and fix dap tests due to API changes in ophyd
  ([`f9ec240`](https://github.com/bec-project/bec/commit/f9ec2403db1dc64d2a975814976f6560336ec184))

- Bugfix within scibec metadata handler to accomodate changes of metadata
  ([`eef2764`](https://github.com/bec-project/bec/commit/eef2764f448b749345e53158ecccf09ea4f463cb))

### Documentation

- Improve docs
  ([`b25a670`](https://github.com/bec-project/bec/commit/b25a6704adf405344b3acfb2417cf5896fa77009))

### Testing

- Fix tests due to config changes
  ([`22c1e57`](https://github.com/bec-project/bec/commit/22c1e5734e0171e8e2a526e947e3f7d8098dad06))


## v2.18.0 (2024-07-03)

### Build System

- Added tomli dependency
  ([`d1b7841`](https://github.com/bec-project/bec/commit/d1b78417c03db383f11385add1362be2a6ce7175))

### Continuous Integration

- Added phoenix, sim and superxas pipelines
  ([`3e91a99`](https://github.com/bec-project/bec/commit/3e91a99945f73bf8fa7b4ddb6dacbab4614d6bdf))

### Features

- **bec_lib**: Added service version tag to service info
  ([`326cd21`](https://github.com/bec-project/bec/commit/326cd218d0a4e1e1444f88964365954fca426900))


## v2.17.6 (2024-07-02)

### Bug Fixes

- **device_server**: Fixed readout of objects that are neither devices nor signals
  ([`b4ee786`](https://github.com/bec-project/bec/commit/b4ee7865cabe9010b49e928d4aa5f6107afd2df4))


## v2.17.5 (2024-07-01)

### Bug Fixes

- **device_server**: Fixed bug that caused alarms not to be raised
  ([`7a5fa85`](https://github.com/bec-project/bec/commit/7a5fa85c0f715602b1edec5b5a499c2139b86b7e))


## v2.17.4 (2024-07-01)

### Bug Fixes

- **rpc**: Fixed bug that caused get to not update the cache
  ([`814f501`](https://github.com/bec-project/bec/commit/814f50132e4018efaafc1f687cc3678bde4af316))

### Refactoring

- **device_server**: Rpc_mixin cleanup
  ([`58c0425`](https://github.com/bec-project/bec/commit/58c0425772e2eee871aecbdb8a9dc88f4c0cb39e))


## v2.17.3 (2024-06-28)

### Bug Fixes

- Bugfix on dtype int/float missmatch for self.positions
  ([`37c4868`](https://github.com/bec-project/bec/commit/37c4868b13df95c56792c89be7171859ba9d9295))

- Fixed cont_line_scan
  ([`d9df652`](https://github.com/bec-project/bec/commit/d9df652e0464ce44eccb4b79c6bc63a54890edef))

### Testing

- Fix tests
  ([`b5ee738`](https://github.com/bec-project/bec/commit/b5ee738153a2fc20d89822018cd420fbab415bba))


## v2.17.2 (2024-06-28)

### Bug Fixes

- Fixed bug where a failed device status would not cause the scan to abort
  ([`2b93187`](https://github.com/bec-project/bec/commit/2b93187c3522e99b09c68bc3b844e3ea6ffd1adf))

### Build System

- Fakeredis dependency version update after fakeredis has been fixed
  ([`33db330`](https://github.com/bec-project/bec/commit/33db33033c4d8028cffe84b154300e926c365315))

### Documentation

- Fix redis install for psi-maintained
  ([`bed9e90`](https://github.com/bec-project/bec/commit/bed9e90183a236880d3e54d93571cdf4ad2ce9a5))


## v2.17.1 (2024-06-25)

### Bug Fixes

- _update_sinks applies different level for each logger
  ([`7ed5d6a`](https://github.com/bec-project/bec/commit/7ed5d6ae82f0605de1f0422a0c6c658cec230159))

- Configure logger levels for BECIPythonClient in constructor
  ([`72b6e3e`](https://github.com/bec-project/bec/commit/72b6e3e543a64d86a615cf400fa5057317a722ad))

- Remove redundant update of loggers
  ([`8b82f35`](https://github.com/bec-project/bec/commit/8b82f357970daab1ad0cac9ea36b42f460b1afd2))

- Set level for each logger to the given value
  ([`1428ba2`](https://github.com/bec-project/bec/commit/1428ba27f9239aa67fcb4b9111980d1d0955de32))

### Refactoring

- Renaming of _update_logger_level to _update_console_logger_level
  ([`03a58d6`](https://github.com/bec-project/bec/commit/03a58d6f1d035cfc0a31d4f6c61436825d0fd31a))


## v2.17.0 (2024-06-25)

### Bug Fixes

- Client: do not configure logging in _start_services()
  ([`4809dc5`](https://github.com/bec-project/bec/commit/4809dc512eec418e08bfa79b40d3b3b75a4498da))

Logging is already configured because BECClient inherits from BECService, and BECService configures
  logging when client is started

- Logger: do not update sinks twice in __init__
  ([`051d6ad`](https://github.com/bec-project/bec/commit/051d6ade9224f5aeb919bbe96e84dc49f4720482))

- Logger: log stderr to sys.__stderr__ to be compatible with sys.stderr redirection
  ([`9824ee4`](https://github.com/bec-project/bec/commit/9824ee43aaf283c743762affead3c3b9e517abce))

- Logger: make console_log opt-in instead of having it by default and removing for certain classes
  ([`1d1f795`](https://github.com/bec-project/bec/commit/1d1f795f9143363fa73a7cc9d5e7827d613552c1))

- **logger**: Do not enqueue log messages
  ([`1318b22`](https://github.com/bec-project/bec/commit/1318b221cb6c26650535019175c74d748b003ea8))

Enqueing log messages is useful when multiple processes (launched with multiprocess module) are
  logging to the same log file, which is not the use case for BEC - it creates processing threads,
  which can be avoided

### Features

- **bec_lib**: Added option to name the logger
  ([`5d6cc7d`](https://github.com/bec-project/bec/commit/5d6cc7dd05ee49e5afd526409fb100b50aa9c56d))

### Testing

- Made completer test more targeted towards the completion results
  ([`cc5503f`](https://github.com/bec-project/bec/commit/cc5503f86c32e266ef4755c78f01eed40cbad808))


## v2.16.3 (2024-06-25)

### Bug Fixes

- **scan_server**: Sync fly scans should not retrieve scan motors
  ([`6dc16b4`](https://github.com/bec-project/bec/commit/6dc16b4a89323c984b77f04cb76eacd442286e5b))


## v2.16.2 (2024-06-25)

### Bug Fixes

- **scan_server**: Ensure that scan server rpc calls use a unique request id
  ([`f3f6966`](https://github.com/bec-project/bec/commit/f3f69669dd15d6d2284afbba336576603d77169b))


## v2.16.1 (2024-06-24)

### Bug Fixes

- **dap**: Fixed auto-run and added e2e test
  ([`5de45d0`](https://github.com/bec-project/bec/commit/5de45d059c7bcfa6e7df769b72128bed7f0dbcda))


## v2.16.0 (2024-06-21)

### Features

- **scan_server**: Added support for additional gui config
  ([`c6987b6`](https://github.com/bec-project/bec/commit/c6987b6ec220ab98690b10bdbeef9823a0c7ed8a))


## v2.15.0 (2024-06-21)

### Features

- **file_writer**: Separated device collection from metadata
  ([`75e6df4`](https://github.com/bec-project/bec/commit/75e6df47f722439df827a307c61849a3828925da))


## v2.14.5 (2024-06-21)

### Bug Fixes

- **bec_lib**: Fixed pydantic type for scanqueuemodifications
  ([`6bf60f9`](https://github.com/bec-project/bec/commit/6bf60f98fcaf80e1ab19ab2752d2d2e71f005225))


## v2.14.4 (2024-06-20)

### Bug Fixes

- Fix bug in emit service info and metrics
  ([`abf77c8`](https://github.com/bec-project/bec/commit/abf77c80804afbb5fbe4d328f88ce4ab88c4710e))

### Documentation

- Added reference to epics configs
  ([`76c2c52`](https://github.com/bec-project/bec/commit/76c2c5285ccc28f701614b9a8aed1b6f03d566ed))

### Testing

- Add tests for metrics
  ([`1ceae8b`](https://github.com/bec-project/bec/commit/1ceae8ba0ce78aa074ea7ed1f0bd374b7ced632f))


## v2.14.3 (2024-06-17)

### Bug Fixes

- **file_writer**: Fixed file writer messages to report successful only after it is written
  ([`27a0f89`](https://github.com/bec-project/bec/commit/27a0f8920ce17116aad10b422d0c5b2ad33ca20c))

### Documentation

- Adjusted init for flyer class
  ([`fa0c96f`](https://github.com/bec-project/bec/commit/fa0c96f2dba82b22395cc91fb5b8fe63956e698c))

- Improved dev install instructions
  ([`d43cd25`](https://github.com/bec-project/bec/commit/d43cd25786aa0e3892592350feb4def8ab541120))

- Moved scanbase code to end of section to not tempt readers to jump directly into the code
  ([`ff9d4ad`](https://github.com/bec-project/bec/commit/ff9d4ad9508ffda81c49977519cf5d2fc95676d7))

### Refactoring

- **scan_server**: Cleanup of scan args
  ([`d61f58c`](https://github.com/bec-project/bec/commit/d61f58c362021f29b937a088b6a0a892cacc9176))


## v2.14.2 (2024-06-12)

### Bug Fixes

- **bec_lib**: Fixed access to global vars
  ([`f621ef2`](https://github.com/bec-project/bec/commit/f621ef280e5121a44277d1b51de586d8eae82be5))


## v2.14.1 (2024-06-12)

### Bug Fixes

- In set_and_publish, do not call set() to not have a warning
  ([`700584c`](https://github.com/bec-project/bec/commit/700584ce3516ba59be56dcfa62cb57a7d693f69f))

- Use endpoints instead of simple strings to avoid warning
  ([`62b2c10`](https://github.com/bec-project/bec/commit/62b2c106de24c5de955fc619fa6b95f949295d21))

### Documentation

- Fixed broken link to hdfgroup
  ([`afbb3ff`](https://github.com/bec-project/bec/commit/afbb3ffb7988573f018ae607ea49ca43331db399))

- Fixed link to file writer docs
  ([`01ac862`](https://github.com/bec-project/bec/commit/01ac8629f50c05c2d69f832b7c2291f50f07a087))


## v2.14.0 (2024-06-09)

### Bug Fixes

- **file_writer**: Set status to running after init
  ([`f4d494b`](https://github.com/bec-project/bec/commit/f4d494b8dc1949842fea9b613b1394af603d29a7))

### Documentation

- Improved file writer docs; added plugin info
  ([`5eefa67`](https://github.com/bec-project/bec/commit/5eefa6726b4e1d0312d2dc04fe36f3d9ba036c0f))

### Features

- **file_writer**: Introduced defaultwriter class to simplify the plugin development
  ([`03c9592`](https://github.com/bec-project/bec/commit/03c9592b6a72689b4c022678528bfd150bc2f837))

### Refactoring

- **file_writer**: Cleanup
  ([`8b5abd4`](https://github.com/bec-project/bec/commit/8b5abd4522424fc898da485c0a9f84018c3d3f08))

### Testing

- **file_writer**: Added tests to load format from plugins
  ([`9adbdaf`](https://github.com/bec-project/bec/commit/9adbdaf0fae5f1f9332790a46073613602c821bc))


## v2.13.8 (2024-06-07)

### Bug Fixes

- Add scan_metadata to documentation
  ([`183152f`](https://github.com/bec-project/bec/commit/183152fac63e174e5db4c7c0b1a064cddc25702e))

- Fix bec.file_writer option to configure writer from command line
  ([`83334f1`](https://github.com/bec-project/bec/commit/83334f18c4ac2c8ce1881ac37231c03022f12442))

### Documentation

- Added two more optional steps to the fly scan tutorial
  ([`ef1a757`](https://github.com/bec-project/bec/commit/ef1a757a248c36aba9e6ef82ca53fb1bab3be3e2))

- Move file_writer to extra section in docs
  ([`8d4a712`](https://github.com/bec-project/bec/commit/8d4a71269be9350d9f9d55395b851d7f9a997253))

- Review documentation for ophyd, scan metadata and file_writer customizations
  ([`cb4a2f6`](https://github.com/bec-project/bec/commit/cb4a2f6e62cbf4d756f575e594722a6971cf5258))

### Refactoring

- Add changes from MR !614; improve kwargs handling for scan requests
  ([`3fece3a`](https://github.com/bec-project/bec/commit/3fece3a063e4b10ed4ed6923a4b7044b0170efb5))

- Add system_config and review docs
  ([`a481fda`](https://github.com/bec-project/bec/commit/a481fdadfe0c1e005b7a9bd35c7a3b8dd15e9756))

- Remove bec.file_writer, and moved info to metadata, renamed md to metadata in kwargs from scans
  ([`92bd51e`](https://github.com/bec-project/bec/commit/92bd51e788233c1597b0aeb317b16642312b9cb0))


## v2.13.7 (2024-06-06)

### Bug Fixes

- Adapt to pytest-redis 3.1
  ([`0a987c0`](https://github.com/bec-project/bec/commit/0a987c0815a3173e43dce22e2accef0e87ea284d))

### Documentation

- Added test instructions to fly scan tutorial
  ([`7cd40ff`](https://github.com/bec-project/bec/commit/7cd40ffcf597e3b64e87d9206468118b400754d7))

- Added tutorial for defining a new fly scan
  ([`df1fe4d`](https://github.com/bec-project/bec/commit/df1fe4d64f97244862126d218be7fe9e2ebea925))

- Refactored scan docs
  ([`08e0978`](https://github.com/bec-project/bec/commit/08e0978d2b7a137700fa1c552cbe079a290f32f5))


## v2.13.6 (2024-06-05)

### Bug Fixes

- Handle redis connection failures more gracefully
  ([`49425c7`](https://github.com/bec-project/bec/commit/49425c7eed456f446c837e09c4fa88afedba31ae))

- **bec_ipython_client**: Fixed support for loading hlis from plugins
  ([`45869aa`](https://github.com/bec-project/bec/commit/45869aab773d4e288f7c2d4152be140f91f5bb79))

### Continuous Integration

- Fixed pytest redis version for now
  ([`c6f1204`](https://github.com/bec-project/bec/commit/c6f12042d3a0d00b1ab9c69a17e829adf76a2c12))


## v2.13.5 (2024-06-05)

### Bug Fixes

- **bec_lib**: Fixed msg type serialization
  ([`05c24e8`](https://github.com/bec-project/bec/commit/05c24e880bfbf2257c973ec4b451f93918290915))


## v2.13.4 (2024-06-05)

### Bug Fixes

- **bec_ipython_client**: Fixed gui startup
  ([`8f4d89e`](https://github.com/bec-project/bec/commit/8f4d89e7a49dc7ca9cbbe64e832ddef19b418f16))


## v2.13.3 (2024-06-04)

### Bug Fixes

- **scan_server**: Fixed order of reported devices in readout priority
  ([`64ecbb6`](https://github.com/bec-project/bec/commit/64ecbb6856de8b108e75f9a4bd2736adb5b4ca74))


## v2.13.2 (2024-06-03)

### Bug Fixes

- **bec_lib**: Fixed serialization for message endpoints
  ([`1be3830`](https://github.com/bec-project/bec/commit/1be38300abcd0c7cc4a5f5dcf3c72cf19deb27d6))


## v2.13.1 (2024-06-03)

### Bug Fixes

- Fixed support for mv during scan defs; closes #308
  ([`835bf50`](https://github.com/bec-project/bec/commit/835bf5004ad1c9aaec1792ed20f3ffc613584d31))


## v2.13.0 (2024-06-03)

### Bug Fixes

- Minor cleanup
  ([`8d4a066`](https://github.com/bec-project/bec/commit/8d4a066832dc45d67b77d13b484d7cd2e565c2f9))

- **bec_lib**: Convert devices to strings for scan requests
  ([`3b176f7`](https://github.com/bec-project/bec/commit/3b176f7b97087fe87fcfaacd4d575c27be4cbcaf))

- **ipython_client**: Readback callback must listen to instruction RID
  ([`c4551d3`](https://github.com/bec-project/bec/commit/c4551d3b953bc97557e285f350e81b000f7c2cbe))

- **scan_server**: Fixed default args
  ([`0f42a49`](https://github.com/bec-project/bec/commit/0f42a4926de28252f01d9f9fab53244cc099ca21))

- **scan_server**: Simplify scan args
  ([`005ff56`](https://github.com/bec-project/bec/commit/005ff5685609b403b35131cdff0380d8e5b2b742))

- **scan_server**: Worker respects use_scan_progress_report
  ([`3ad46ef`](https://github.com/bec-project/bec/commit/3ad46efb148ab9c32e34a6500f1f1af0dbd7144c))

### Documentation

- Improved scan stub docs and glossary
  ([`e04cf65`](https://github.com/bec-project/bec/commit/e04cf65f9cbcff4ea8fe3676813e4dce663757a4))

### Features

- **scan_server**: Added set_with_response and request_is_completed stubs
  ([`8ac80c1`](https://github.com/bec-project/bec/commit/8ac80c11ce0e83bb782254b06e2552e8a15c1002))

- **scan_server**: Convert arg inputs to supported scan arg types
  ([`30b4528`](https://github.com/bec-project/bec/commit/30b4528de5e448a0c3477d49dff727703de3ed17))

### Refactoring

- **scan_server**: Cleanup of scan args
  ([`3acc13a`](https://github.com/bec-project/bec/commit/3acc13a8c4fa45765c1b29f446c01df21b056135))

### Testing

- Added tests for stubs and contlineflyscan
  ([`8fed5f6`](https://github.com/bec-project/bec/commit/8fed5f64a09ea28bb911aaf57a96ba4b50498a56))

- **scan_server**: Added test for convert_arg_input
  ([`a302844`](https://github.com/bec-project/bec/commit/a302844d70659e2d1b074a76c2649a2c15bf0754))


## v2.12.6 (2024-05-31)

### Bug Fixes

- End the color sequence
  ([`22be4c4`](https://github.com/bec-project/bec/commit/22be4c4c6b54133277411e837e9c102aa39685d3))


## v2.12.5 (2024-05-28)

### Bug Fixes

- Remove deprecated arg speed from deviceconfig
  ([`67f0bea`](https://github.com/bec-project/bec/commit/67f0beac75bbeecf69768662e373b96a0839f122))


## v2.12.4 (2024-05-28)

### Bug Fixes

- Create readme for tests_dap_services
  ([`104c847`](https://github.com/bec-project/bec/commit/104c847b55427c3ac78afb3af9e71154deff7d9e))

### Continuous Integration

- Added development pages
  ([`4a9f4f8`](https://github.com/bec-project/bec/commit/4a9f4f83fae16f40df679cddc5bf816e3b77deff))

### Documentation

- Added docs for developing scans
  ([`5f44521`](https://github.com/bec-project/bec/commit/5f4452110519404573484d2c6a95d8a46c325a1f))

- Added linkify
  ([`3a363f5`](https://github.com/bec-project/bec/commit/3a363f5f52b644bc2542913cf4e9acf224ef69f9))

- Added reference to gitlab issues
  ([`7277ac3`](https://github.com/bec-project/bec/commit/7277ac3c40f86ff465f7af69a060fb9d5f2d4acc))

- Added reference to user docs for loading new device configs
  ([`fd29dfb`](https://github.com/bec-project/bec/commit/fd29dfb5f7d63d864e08adae1b5128f0f0fed14a))

- Cleanup
  ([`7254755`](https://github.com/bec-project/bec/commit/7254755aacda0f9c50b09237a59cd3584fb48e74))

- Fixed api reference
  ([`29862dc`](https://github.com/bec-project/bec/commit/29862dca51873d4c22db6a693014ecf7addb4447))

- Fixed api reference; added reference to scanbase
  ([`121e592`](https://github.com/bec-project/bec/commit/121e5922eb3806eff88f49b5378b1f12056be132))

- Fixed broken links
  ([`5dfcbe6`](https://github.com/bec-project/bec/commit/5dfcbe6d132dd199be9f42980ed254efb2dc0e82))

- Fixed dependency for building sphinx
  ([`9cbde72`](https://github.com/bec-project/bec/commit/9cbde72505723b5e4da94eeab4c8313e29c295c5))

- Improvements for the dev docs
  ([`e5a98d7`](https://github.com/bec-project/bec/commit/e5a98d718d06004819b32db1fabf77e634bdefd0))

- Restructured developer docs
  ([`7fd66f8`](https://github.com/bec-project/bec/commit/7fd66f895905cb3e46ee90b98bfac8985837d6ca))

### Refactoring

- Deprecated scan report hint
  ([`0382ac5`](https://github.com/bec-project/bec/commit/0382ac52dd9d68e6871866311416632ee39ed232))

- Renamed move_and_wait to move_scan_motors_and_wait
  ([`eaa8bd8`](https://github.com/bec-project/bec/commit/eaa8bd8e67aa75a00d6a5b3e2494ed9828e7d6cf))


## v2.12.3 (2024-05-21)

### Bug Fixes

- Renamed scan_progress to device_progress
  ([`d344e85`](https://github.com/bec-project/bec/commit/d344e8513781f29a1390adc92826f23d1702964b))

- Renamed table_wait to scan_progress
  ([`855f9a8`](https://github.com/bec-project/bec/commit/855f9a8412e9c0d8b02d131ece533b4d85882b36))


## v2.12.2 (2024-05-17)

### Bug Fixes

- **scihub**: Added experimentId to scan entries in BEC db
  ([`8ba7213`](https://github.com/bec-project/bec/commit/8ba7213e29ac0335bca126b9d8a08a9ec46e469f))


## v2.12.1 (2024-05-17)

### Bug Fixes

- Clean all imports from bec_lib, remove use of @threadlocked
  ([`8a017ef`](https://github.com/bec-project/bec/commit/8a017ef3d7666f173a70f2e6a8606d73b1af0095))

- Do not import modules if only for type checking (faster import)
  ([`1c628fd`](https://github.com/bec-project/bec/commit/1c628fd6105ef5df99e97c8945d3382c45ef5350))

- Import 'dap_plugin_objects' at last minute to speed up initial import
  ([`d7db6be`](https://github.com/bec-project/bec/commit/d7db6befe9b8e4689ed37ccad44f8a5d06694180))

- Messages import made lazy to speed up initial import time
  ([`791be9b`](https://github.com/bec-project/bec/commit/791be9b25aa618d508feed99a201e0c58b56f3ce))

TODO: put back imports as normal when Pydantic gets faster

- Race condition when reading new value from stream
  ([`87cc71a`](https://github.com/bec-project/bec/commit/87cc71aa91c9d35b6483f4ef6c5de3c59575e9dc))

- Solve scope problem with 'name' variable in lambda
  ([`417e73e`](https://github.com/bec-project/bec/commit/417e73e5d65f0c774c92889a44d1262c7f4f343b))

- Use lazy import to reduce bec_lib import time
  ([`649502e`](https://github.com/bec-project/bec/commit/649502e364e4e4c0ea53f932418c479f2d6978d4))


## v2.12.0 (2024-05-16)

### Features

- **scan_bundler**: Added scan progress
  ([`27befe9`](https://github.com/bec-project/bec/commit/27befe966607a3ae319dbee3af9e59ef0d044bc8))


## v2.11.1 (2024-05-16)

### Bug Fixes

- **bec_lib**: Fixed loading scripts from plugins
  ([`3264434`](https://github.com/bec-project/bec/commit/3264434d40647d260400045f7bbd4c2ee9bb2c4e))

User scripts from plugins were still relying on the old plugin structure

### Continuous Integration

- Cleanup ARGs in dockerfiles
  ([`b670d1a`](https://github.com/bec-project/bec/commit/b670d1aa6b6e2af0cb09e7dbc77ea5d1bc66593b))

- Run AdditionalTests jobs on pipeline start
  ([`c9ece7e`](https://github.com/bec-project/bec/commit/c9ece7ef2f1f9b052ed9b92bcb29463cf8371c64))

This is a followup to !573

### Documentation

- **bec_lib**: Improved scripts documentation
  ([`79f487e`](https://github.com/bec-project/bec/commit/79f487ea8b9dc135102204872390631e59a60e54))


## v2.11.0 (2024-05-15)

### Code Style

- Create directory to contain utils
  ([`549994d`](https://github.com/bec-project/bec/commit/549994d0fdffcd4f5ed0948e1cd4cd4a0d0092af))

### Features

- Add 'Proxy' to bec_lib utils
  ([`11a3f6d`](https://github.com/bec-project/bec/commit/11a3f6daa46b3e6a82b66bd781b7590d01478b54))

- Add utilities to lazy import a module
  ([`a37ae57`](https://github.com/bec-project/bec/commit/a37ae577f68c154dc3da544816b7c7f0cb532c50))

- Add utility function to determine instance of an object by class name
  ([`0ccd13c`](https://github.com/bec-project/bec/commit/0ccd13cd738dc12d4a587b4c5e0d6b447d7cfc50))


## v2.10.4 (2024-05-14)

### Bug Fixes

- Disabled script linter for now
  ([`5c5c18e`](https://github.com/bec-project/bec/commit/5c5c18ef0eab33ebaa33d1a0daa846ea7f2f59a8))

### Build System

- Fixed fakeredis version for now
  ([`51dfe69`](https://github.com/bec-project/bec/commit/51dfe69298170eba7220fcb506d99515c46ea32a))

### Continuous Integration

- Update dependencies and add ci job to check for package versions
  ([`2aafb24`](https://github.com/bec-project/bec/commit/2aafb249e8f0b8afa8ede0dc4ba0a811ecb2a70f))


## v2.10.3 (2024-05-08)

### Bug Fixes

- Upgraded to ophyd_devices v1
  ([`3077dbe`](https://github.com/bec-project/bec/commit/3077dbe22ae50e6aae317c72022df6ea88b14cce))


## v2.10.2 (2024-05-08)

### Bug Fixes

- **RedisConnector**: Add 'from_start' support in 'register' for streams
  ([`f059bf9`](https://github.com/bec-project/bec/commit/f059bf9318038404ebbcc82b5abf5cd148486021))

### Continuous Integration

- Added ds pipeline for tomcat
  ([`55d210c`](https://github.com/bec-project/bec/commit/55d210c7ae06ea509328510e6aec636caf009cfd))

### Refactoring

- **bec_startup**: Default gui is BECDockArea (gui variable) with fig in first dock
  ([`7dc2426`](https://github.com/bec-project/bec/commit/7dc242689f0966d692d3aeb77ca7689ea8709680))


## v2.10.1 (2024-05-07)

### Bug Fixes

- Upgraded plugin setup tools
  ([`ea38501`](https://github.com/bec-project/bec/commit/ea38501ea7ae4a62d6525b00608484ff1be540a1))

### Build System

- Fixed dependency range
  ([`c10ac5e`](https://github.com/bec-project/bec/commit/c10ac5e78887844e46b965a707351d663ac4bcf8))

### Continuous Integration

- Changed repo name to bec_widgets in downstream tests
  ([`698029b`](https://github.com/bec-project/bec/commit/698029b637b1c84c5b1e836d8c6fbc8c8c7e3e0e))

- Moved from multi-project pipelines to parent-child pipelines
  ([`9eff5ca`](https://github.com/bec-project/bec/commit/9eff5ca3580c3536e1edff5ade264dc6fc3f6f6e))


## v2.10.0 (2024-05-03)

### Features

- Add client message handler to send info messages from services to clients; closes 258
  ([`c0a0e3e`](https://github.com/bec-project/bec/commit/c0a0e3e44299b350790687db436771c6b456567a))


## v2.9.6 (2024-05-02)

### Bug Fixes

- **scihub**: Fixed scibec connector for new api
  ([`fc94c82`](https://github.com/bec-project/bec/commit/fc94c827e40f12293c59b139ccd455df8b8b4d70))


## v2.9.5 (2024-05-02)

### Bug Fixes

- Do not try to populate `user_global_ns` if IPython interpreter is not there
  ([`cf07feb`](https://github.com/bec-project/bec/commit/cf07febc5cf0fdadec0e9658c2469ce1adb1a369))

- Use the right redis fixture in "bec_servers" fixture to prevent multiple redis processes to be
  started
  ([`51d65e2`](https://github.com/bec-project/bec/commit/51d65e2e9547765c34cc4a0a43f1adca90e7e5c3))

### Testing

- Added more tests for scan queue
  ([`b664b92`](https://github.com/bec-project/bec/commit/b664b92aae917d2067bfca48a60eeaf44ced0c98))


## v2.9.4 (2024-05-01)

### Bug Fixes

- Unified device message signature
  ([`c54dfc1`](https://github.com/bec-project/bec/commit/c54dfc166fe9dd925b15e8cc8750cebaec8896cb))

### Refactoring

- Added isort params to pyproject
  ([`0a1beae`](https://github.com/bec-project/bec/commit/0a1beae06ae128d9817272644d2f38ca761756ab))

- **bec_lib**: Cleanup
  ([`6bf0998`](https://github.com/bec-project/bec/commit/6bf0998c71387307ad8d842931488ec2aea566a8))


## v2.9.3 (2024-05-01)

### Bug Fixes

- Device_req_status only needs set
  ([`587cfcc`](https://github.com/bec-project/bec/commit/587cfccbe576dcd2eb10fc16e225ee3175f8d2a0))

- Fixed log message log type
  ([`af85937`](https://github.com/bec-project/bec/commit/af8593794c2ea9d0b4851b367aca4e6546fc760f))

- Fixed log message signature and added literal checks; closes #277
  ([`ca7c238`](https://github.com/bec-project/bec/commit/ca7c23851976111d81c811bf16b6d6f371d24dc6))

- Logs should be send, not set_and_publish; closes #278
  ([`3964870`](https://github.com/bec-project/bec/commit/396487074905930c410978144e986d1b9b373a2c))


## v2.9.2 (2024-04-29)

### Bug Fixes

- **bec_startup**: Becfigure starts up after client
  ([`6b48858`](https://github.com/bec-project/bec/commit/6b488588fed818ee1fefae8d5620821381b2eee0))


## v2.9.1 (2024-04-29)

### Bug Fixes

- Renamed dap_services to services
  ([`62549f5`](https://github.com/bec-project/bec/commit/62549f57c9a497f0feceb63a8facd66669f56437))

- Updated plugin helper script to new plugin structure
  ([`8e16efb`](https://github.com/bec-project/bec/commit/8e16efb21a5f6f68eee61ff22a930bf9e7400110))

### Documentation

- Updated docs for bec plugins
  ([`29b89dd`](https://github.com/bec-project/bec/commit/29b89dd0173dfd9a692040d0acbf14bf47a6a46c))


## v2.9.0 (2024-04-29)

### Documentation

- Added section on logging
  ([`ebcd2a4`](https://github.com/bec-project/bec/commit/ebcd2a4dbc2a52dc1e8679e54784daa0f6a3901b))

### Features

- **bec_lib**: Added log monitor as CLI tool
  ([`0b624a4`](https://github.com/bec-project/bec/commit/0b624a4ab5039c157edc1a3b589ba462f82879dd))

- **bec_lib**: Added trace log with stack trace
  ([`650de81`](https://github.com/bec-project/bec/commit/650de811090dc72407cfb746eb22aa883682d268))

### Testing

- **bec_lib**: Added test for log monitor
  ([`64d5c30`](https://github.com/bec-project/bec/commit/64d5c304d98c04f5943dd6365de364974a6fc931))


## v2.8.0 (2024-04-27)

### Build System

- Fixed fpdf version
  ([`94b6995`](https://github.com/bec-project/bec/commit/94b6995fd32224557b2fc8b3aeafcf73acdb8a2c))

### Features

- **bec_lib**: Added option to combine yaml files
  ([`39bb628`](https://github.com/bec-project/bec/commit/39bb6281bda2960de7e70c45463f62dde2b454f5))


## v2.7.3 (2024-04-26)

### Bug Fixes

- Fixed loading of plugin-based configs
  ([`f927735`](https://github.com/bec-project/bec/commit/f927735cd4012d4e4182596dc2ac2735d5ec4697))

### Documentation

- Fixed bec config template
  ([`87d0986`](https://github.com/bec-project/bec/commit/87d0986f21ba367dbb23db50c7c13f10b4007030))

- Review docs, fix ScanModificationMessage, monitor callback and DAPRequestMessage
  ([`6b89240`](https://github.com/bec-project/bec/commit/6b89240f46b2f892847e81963b7898649cb1c8d9))

### Testing

- **bec_lib**: Added test for unregistering callbacks
  ([`6e14de3`](https://github.com/bec-project/bec/commit/6e14de35dc43b7eed3244f5fe327d79ddc1302ae))


## v2.7.2 (2024-04-25)

### Bug Fixes

- **channel_monitor**: Register.start removed since connector.register do not have any .start method
  ([`1eaefc1`](https://github.com/bec-project/bec/commit/1eaefc1c8ab08e8c4939c05912d476b08bdcc2c9))

- **redis_connector**: Unregister is not killing communication
  ([`b31d506`](https://github.com/bec-project/bec/commit/b31d506c9f7b541e0b8022aafdb8d44e0478ea3c))

### Refactoring

- Add file_writer and readme for tests
  ([`d8f76f5`](https://github.com/bec-project/bec/commit/d8f76f505726fe12bdf572a9b5659a3c04620fde))


## v2.7.1 (2024-04-23)

### Bug Fixes

- Fixed device server startup for CA override
  ([`773572b`](https://github.com/bec-project/bec/commit/773572b33b23230b06ea6cc7b8e7e5ab3f792f3e))


## v2.7.0 (2024-04-19)

### Continuous Integration

- Removed pipeline as trigger source for downstream jobs
  ([`92bb7ef`](https://github.com/bec-project/bec/commit/92bb7ef3c59f14d25db63615a86445454201aafd))

- Skip trailing comma for black
  ([`fe657b6`](https://github.com/bec-project/bec/commit/fe657b6adc416e7bc63b0a1e2970fdddcca63c29))

- Update default ophyd branch to main
  ([`3334a7f`](https://github.com/bec-project/bec/commit/3334a7f8e70d220daeaef51ac39328e3019a9bf0))

### Features

- Move cSAXS plugin files from core
  ([`0a609a5`](https://github.com/bec-project/bec/commit/0a609a56c0295026d04c4f5dea51800ad4ab8edf))


## v2.6.0 (2024-04-19)

### Continuous Integration

- Fixed build process during e2e test
  ([`369af7c`](https://github.com/bec-project/bec/commit/369af7c2006114ece464f5cf96c332c059ab3154))

- Stop after two failures
  ([`90b7f45`](https://github.com/bec-project/bec/commit/90b7f45c135f63b7384ef5feaee71902fb11ec74))

### Documentation

- Fixed version update for sphinx
  ([`8366896`](https://github.com/bec-project/bec/commit/836689667c03c0aa1a35db97ca772f2ae05f5f79))

- **dev/install**: Fixed install guide for developers bec_client -> bec_ipython_client
  ([`a8d270e`](https://github.com/bec-project/bec/commit/a8d270e0d702e4750b63631bf9fb34e4f30ed610))

### Features

- **bec_client**: Added support for plugin-based startup scripts
  ([`aec75b4`](https://github.com/bec-project/bec/commit/aec75b4966e570bd3e16ac295b09009eb1589acd))

- **bec_lib**: Added plugin helper
  ([`7f1b789`](https://github.com/bec-project/bec/commit/7f1b78978bbe2ad61e490416e44bc23001757d5e))

- **file_writer**: Added support for file writer layout plugins
  ([`a6578fb`](https://github.com/bec-project/bec/commit/a6578fb13349c0cabd24d313a7d58f63772fa584))

- **scan_server**: Added support for plugins
  ([`23f8721`](https://github.com/bec-project/bec/commit/23f872127b06d321564fa343b069ae962ba2b6c6))

### Refactoring

- Minor cleanup
  ([`b7bd584`](https://github.com/bec-project/bec/commit/b7bd584898a8ca6f11ff79e11fda2727d0fc6381))

- Moved to dot notation for specifying device classes
  ([`1f21b90`](https://github.com/bec-project/bec/commit/1f21b90ba31ec8eb8ae2922a7d1353c2e8ea48f6))

- Removed outdated xml writer
  ([`c9bd092`](https://github.com/bec-project/bec/commit/c9bd0928ea9f42e6b11aadd6ac42d7fe5e649ec7))


## v2.5.0 (2024-04-18)

### Build System

- Fix path to bec_ipython_client version
  ([`4420148`](https://github.com/bec-project/bec/commit/4420148a09e2f92354aa20be75a9d3b0f19f4514))

- Moved to pyproject
  ([`f7f7eba`](https://github.com/bec-project/bec/commit/f7f7eba2316ec78f2f46a59c52234f827d509101))

- Removed wheel dependency
  ([`ff0d2a1`](https://github.com/bec-project/bec/commit/ff0d2a1ebb266d08d93aa088ff3151d27c828446))

- **bec_lib**: Upgraded to fpdf2
  ([`c9818c3`](https://github.com/bec-project/bec/commit/c9818c35e4b1f3732ae6403c534bb505ad1121fc))

### Continuous Integration

- Exit job if no artifacts need to be uploaded to pypi
  ([`2e00112`](https://github.com/bec-project/bec/commit/2e00112447e5aee5ce91bc0fa9f51e9faf0f4ee5))

- Migrate docker to gitlab Dependency Proxy
  ([`80270f8`](https://github.com/bec-project/bec/commit/80270f81968bfb717a0c631f0a87a0b809912f6a))

Related to 1108662db13e8142b37cb3645ff7e9bc94d242b8

The docker-compose file/command might need further fixes, once the related end-2-end tests are
  activated.

- Updated ci for pyproject
  ([`3b541fb`](https://github.com/bec-project/bec/commit/3b541fb7600e499046d053f21a399de01263fb24))

### Features

- Added pytest-bec-e2e plugin
  ([`deaa2b0`](https://github.com/bec-project/bec/commit/deaa2b022ae636d77401f905ed522024b44721f5))

### Testing

- **device_server**: Fixed leaking threads in device server tests
  ([`ae65328`](https://github.com/bec-project/bec/commit/ae653282bc107077f54e79b822e9dea188d53eca))


## v2.4.2 (2024-04-16)

### Bug Fixes

- **ci**: Add rules to trigger child pipelines
  ([`5a1894b`](https://github.com/bec-project/bec/commit/5a1894bfca881b9791704c8a6aeb274e2f002a51))

### Continuous Integration

- Pull images via gitlab dependency proxy
  ([`1108662`](https://github.com/bec-project/bec/commit/1108662db13e8142b37cb3645ff7e9bc94d242b8))


## v2.4.1 (2024-04-16)

### Bug Fixes

- **client**: Resolve on done
  ([`5ea7ed3`](https://github.com/bec-project/bec/commit/5ea7ed3e3e4b7b9edfff5008321eaf5e5cdaf9ae))

### Continuous Integration

- Updated default BECWidgets branch name to main
  ([`c41fe08`](https://github.com/bec-project/bec/commit/c41fe0845532a05a7dfbd2f9aec038b1801e29c3))


## v2.4.0 (2024-04-15)

### Continuous Integration

- Remove AdditionalTests dependency on tests job
  ([`54b139f`](https://github.com/bec-project/bec/commit/54b139f40cebba03f1302f7828d30a9602cc807d))

### Features

- **flomni**: Scan status for tomography
  ([`eca3e64`](https://github.com/bec-project/bec/commit/eca3e64facd2b1faa46787d9d70f8ce027df645f))


## v2.3.0 (2024-04-12)

### Continuous Integration

- Changes due to renaming of master to main
  ([`291779f`](https://github.com/bec-project/bec/commit/291779f4c362b5241b5ca636408cb4b36e4f551d))

- Specify main branch for semver job
  ([`31a54ca`](https://github.com/bec-project/bec/commit/31a54cab9325fa0932a2189b4032404036cfbbe6))

### Features

- Rename spec_hli to bec_hli, add load_hli function to BECIPythonCLient; closes #263
  ([`6974cb2`](https://github.com/bec-project/bec/commit/6974cb2f13e865d1395eda2274ac25abd6e44ef8))

### Refactoring

- Use callback_handler for namespace updates of clients and add tests
  ([`0a832a1`](https://github.com/bec-project/bec/commit/0a832a149dbbc37627ff84674a6d38f5697db8ab))


## v2.2.1 (2024-04-12)

### Bug Fixes

- **client**: Removed outdated bec plotter; to be replaced by BECFigure once ready
  ([`52b33d8`](https://github.com/bec-project/bec/commit/52b33d8b65a9496fa38719cb30ba5666cccd4b55))

### Documentation

- Added link to BECFigure docs
  ([`6d13618`](https://github.com/bec-project/bec/commit/6d13618a6fec7104bcb72cb32745ad645851bec3))


## v2.2.0 (2024-04-11)

### Features

- Add bec_service names to log files
  ([`329e9ed`](https://github.com/bec-project/bec/commit/329e9eda5b31f033af4535c01545b4d1ceeb12c6))


## v2.1.0 (2024-04-11)

### Bug Fixes

- .shutdown() will cleanly stop all threads
  ([`c1c7dd7`](https://github.com/bec-project/bec/commit/c1c7dd7beaeb46d1ababd301b99d01266baeb26c))

- Ensure "newest_only" works as expected in test
  ([`dc85f49`](https://github.com/bec-project/bec/commit/dc85f494ed93727e7eb3b207cdddb2db60ceb3f5))

- **redis connector**: Prevent multiple identical connections in 'register'
  ([`344ef50`](https://github.com/bec-project/bec/commit/344ef508c0be199d5d8ab9b4c4bff3e4778acb87))

- **redis_connector**: Support dict in convert_endpointinfo
  ([`d2942b1`](https://github.com/bec-project/bec/commit/d2942b1436ed7ddc3c31feb61510f0dc9f6f7f5a))

- **test_fake_redis**: Testmessage fixed to pydantic BaseModel
  ([`eb9c812`](https://github.com/bec-project/bec/commit/eb9c8125290615c0e15ffa70567ff198d22c30d5))

### Continuous Integration

- **bec-widgets**: Environmental variable added to test script for ci
  ([`8e2fa9b`](https://github.com/bec-project/bec/commit/8e2fa9b910e2d52da60b0e4db00e608b511eb7ee))

### Features

- **connector**: Add 'unregister' method to cancel subscription to pub/sub
  ([`e87812a`](https://github.com/bec-project/bec/commit/e87812a816d06cd19e23705ff4221efe261b588c))

- **redis connector**: Add _execute_callback method, to be able to overwrite how callbacks are run
  ([`1ddc7ee`](https://github.com/bec-project/bec/commit/1ddc7eec53994e793cee371cae64474136faf963))

### Refactoring

- (un)register to work with pub/sub or stream endpoints
  ([`93a5a28`](https://github.com/bec-project/bec/commit/93a5a2854b7408f7ff4ba32863f2cb3918b885e5))

- Make '(un)register_stream' similar to pub/sub registration API
  ([`e1ad412`](https://github.com/bec-project/bec/commit/e1ad412be7d224b6169db7cf45b105b287334781))

- StreamTopicInfo renamed to StreamSubscriptionInfo - "cb" field renamed to "cb_ref" (because it is
  really a weakref) - removed StreamRegisterMixin class - merged with RedisConnector, since there is
  no other class to mix with - removed need for custom stream listeners - differenciation between
  'direct' reading and 'bunch' reading is made with a specialized StreamSubscriptionInfo object
  called 'DirectReadingStreamSubscriptionInfo' - use a single events queue for all messages - all
  messages callbacks treated the same, by the same thread - pay attention to registering multiple
  times a stream to the same callback, and prevent newest_only=True streams to also be registered
  with the same callback with newest_only=False


## v2.0.3 (2024-04-11)

### Bug Fixes

- Fixed entry points
  ([`82b4689`](https://github.com/bec-project/bec/commit/82b4689beb96b3a11ea1c2d5203167cb45746ffa))

### Documentation

- **developer**: Updated developer instructions after bec server refactoring
  ([`792c5cd`](https://github.com/bec-project/bec/commit/792c5cdb95e7838d3198171e0dac2533ba73a8a4))


## v2.0.2 (2024-04-11)

### Bug Fixes

- Add raise condition for fetching path from service_config for recovery_device_config dumps
  ([`0a9a674`](https://github.com/bec-project/bec/commit/0a9a6747da87e318ed8ec6e4c6e594f05fa7070a))

### Build System

- Fixed install script
  ([`9813e51`](https://github.com/bec-project/bec/commit/9813e51f878504d28c3c3f6c11098570c9d78b70))


## v2.0.1 (2024-04-11)

### Bug Fixes

- Fixed build during semver job
  ([`0bb8cb0`](https://github.com/bec-project/bec/commit/0bb8cb0bfbf2ce9ac69f7522ddf92e68eb2aa1e4))


## v2.0.0 (2024-04-10)

### Continuous Integration

- Fixed semver job
  ([`905c46a`](https://github.com/bec-project/bec/commit/905c46a085a2310fb99aa87f63a8ab17290149cf))

- Removed test utils from coverage
  ([`e9e366c`](https://github.com/bec-project/bec/commit/e9e366ca1c33e701e1fe7addcf23d2cd0ad58fe1))

### Refactoring

- Moved services to bec_server
  ([`405d12e`](https://github.com/bec-project/bec/commit/405d12e74a3c7a27aa0e357a1d8438dc5f35b079))

All services are now in the bec_server package. This is a breaking change as the standalone import
  of the services will no longer work.


## v1.24.1 (2024-04-10)

### Bug Fixes

- **flomni**: Wait for cleanup to be finished
  ([`8660096`](https://github.com/bec-project/bec/commit/8660096e536f59f04b8fb3f179cdcf7bf078b1cf))

- **scan_server**: Break out of run loop if signal event is set
  ([`6edac2f`](https://github.com/bec-project/bec/commit/6edac2f54d1d1c33f8d1e7329298361a5b1c62f1))

- **scan_server**: Error during return_to_start was not caught and caused the scan worker to shut
  down
  ([`1fa372b`](https://github.com/bec-project/bec/commit/1fa372b59023210d602c5d5627e34107418e14b7))

- **scan_server**: Reset worker to running after failed cleanup
  ([`fa6f2da`](https://github.com/bec-project/bec/commit/fa6f2da8af434517da2001df9775a2b834132ec3))

- **scan_server**: Restart queue if worker died
  ([`a59eb9c`](https://github.com/bec-project/bec/commit/a59eb9c8886a410f0dbaa4351b1b37de72dbdc20))

- **scan_server**: Set queue to stopped after reaching a limit error
  ([`8470f63`](https://github.com/bec-project/bec/commit/8470f636a2d1db2ff6f1dd68e7db0e18555c085b))

### Code Style

- **scan_server**: Fixed formatter
  ([`9ae8a29`](https://github.com/bec-project/bec/commit/9ae8a29fe4ed2b1e7b90c58c321180489a2a67e1))

### Refactoring

- **flomni**: Fixed formatter
  ([`bb1d138`](https://github.com/bec-project/bec/commit/bb1d1380e4f0d8a3898b922e5aef5371ad82bc6d))


## v1.24.0 (2024-04-10)

### Features

- Add check for logger to load correct config, add tests
  ([`2317fd3`](https://github.com/bec-project/bec/commit/2317fd3df797468dcd583c70221f65a5b2f5ea9b))

### Refactoring

- Moved messages to pydantic
  ([`95ac205`](https://github.com/bec-project/bec/commit/95ac2055eb30015d9690faf004c5665fa8a4a555))


## v1.23.1 (2024-04-09)

### Bug Fixes

- Add random order to full end-2-end tests
  ([`8a47f76`](https://github.com/bec-project/bec/commit/8a47f76f25b65b9252fa90055074267e207512ae))

- Fix logs for ci pipeline
  ([`28d3dda`](https://github.com/bec-project/bec/commit/28d3dda2c398baa7251da0b64e42b7704177ffc9))

### Refactoring

- Renamed pointID to point_id
  ([`d08526f`](https://github.com/bec-project/bec/commit/d08526f6e9992bfd08a987b8ff4e0d741f558e8e))

- **bec_lib**: Prevent devices to appear in multiple args
  ([`be2330c`](https://github.com/bec-project/bec/commit/be2330c2219073d6d047b7a5702be8b23994f20e))

- **scan_bundler**: Added more logger outputs
  ([`8642495`](https://github.com/bec-project/bec/commit/8642495afa651ba4d70edcc4baf499c44c621eba))


## v1.23.0 (2024-04-08)

### Bug Fixes

- Fix .deepcopy vs copy for metadata from client
  ([`9ad68ab`](https://github.com/bec-project/bec/commit/9ad68ab69a67b1fce61682a6fd24716df10b2208))

### Features

- Adapt file writing; log files to common dir and refactoring of filewriter
  ([`246f271`](https://github.com/bec-project/bec/commit/246f271bc9404d38e4100c8dbd0094af7b1136f6))


## v1.22.2 (2024-04-08)

### Bug Fixes

- **issue #253**: Split startup in bec entry point in 2 parts, ensure globals are in IPython
  namespace
  ([`42625c3`](https://github.com/bec-project/bec/commit/42625c357e0a74824f79ef59d22cd622da4e4d52))

### Documentation

- **developer**: Fixed isort description
  ([`cb41c6f`](https://github.com/bec-project/bec/commit/cb41c6f1acadd8652634b76c58a740046f7bf834))


## v1.22.1 (2024-04-04)

### Bug Fixes

- **bec_client**: Unnecessary complex exit thread
  ([`9377a84`](https://github.com/bec-project/bec/commit/9377a84f8b3e3ffc70bd81a08878d634a4f63db7))

### Continuous Integration

- Added isort to ci checks
  ([`7ebf090`](https://github.com/bec-project/bec/commit/7ebf090f156f0434a0faee84d3ca4aa181d48319))

- Allow failure of multi-host test to avoid blocking the pipeline
  ([`23c0e78`](https://github.com/bec-project/bec/commit/23c0e783117aaa88eaaa3065e16bd4a8a4df0138))

- Fixed support for ophyd branches other than master
  ([`3bbb03b`](https://github.com/bec-project/bec/commit/3bbb03bbfacc29b1a2c7f7beff96b85041a3c8ab))

### Documentation

- Adapt user installation; closes #246
  ([`e669252`](https://github.com/bec-project/bec/commit/e6692524136595ed20640db49db4420278e3d5cc))

- Add documentation for pytest fixtures provided as pytest plugins
  ([`4b3851e`](https://github.com/bec-project/bec/commit/4b3851e2825c5676e6ae8cdf4334296a69546d02))

- Added isort to developer instructions
  ([`720e3c3`](https://github.com/bec-project/bec/commit/720e3c39eec764824efe5e30edebd449fab1e92d))

- Address comments
  ([`2de2e1b`](https://github.com/bec-project/bec/commit/2de2e1b4bc5fb4cd3060e082fd641dc1c9cafb74))

- Fix wording
  ([`8dd3ee0`](https://github.com/bec-project/bec/commit/8dd3ee0946728fe394e8863d04934d70c1e96ba7))

- Refactor summary, configure in docs
  ([`536f2ef`](https://github.com/bec-project/bec/commit/536f2efc0cbc9e877dfc6c908e413910e5460cb8))

- Update documentation about ophyd devices
  ([`04b3bb0`](https://github.com/bec-project/bec/commit/04b3bb03b9c9697217e7b2fa703581a29a8b61f4))

### Refactoring

- Bec startup script using setup entry point
  ([`ca16c1b`](https://github.com/bec-project/bec/commit/ca16c1bb6ed7430ad49340478d315032962352fc))


## v1.22.0 (2024-03-28)

### Bug Fixes

- Temporary fix, do not check for dangling threads in device server tests
  ([`a1cddc0`](https://github.com/bec-project/bec/commit/a1cddc0639b509531341beb24003479158ea8cd7))

- Temporary make 'end-2-end tests with multiple hosts' manual
  ([`54bfe36`](https://github.com/bec-project/bec/commit/54bfe36fe6becc80b3b893def94a14e0ed1ecdab))

- **tests**: Ensure all tests do not leak threads
  ([`f371098`](https://github.com/bec-project/bec/commit/f37109873a0c173dc56a254fdcfca52d8c3ad215))

- **tests**: Rename files to prevent error during tests collection with 2 files with same name
  ([`6fcef45`](https://github.com/bec-project/bec/commit/6fcef459b855968e3e179d1d4ae48a0f00487bd2))

### Build System

- Added isort to setups and pre-commit
  ([`424377b`](https://github.com/bec-project/bec/commit/424377b8f2cee203aa1f1422cf8704bd20533556))

### Code Style

- Apply isort to the whole codebase, clean unused imports
  ([`2d66967`](https://github.com/bec-project/bec/commit/2d66967e04a3cbcecde4027d4f0f5ecb1c3c640d))

- Fix pep8 compliance
  ([`9b6ac54`](https://github.com/bec-project/bec/commit/9b6ac547e7158df9351f485543c849419bc00858))

### Continuous Integration

- Added CI variables for downstream pipelines and web source
  ([`b3ebe4e`](https://github.com/bec-project/bec/commit/b3ebe4e35dc29c03caa4e3a7dae3bdc24542bbe0))

- Added web as trigger for downstream pipelines
  ([`b46aac7`](https://github.com/bec-project/bec/commit/b46aac7a8ae6d3eeacacb7dc673e42ad7f55c97f))

### Documentation

- **scan_server**: Improved docs for scan stubs
  ([`c5f18e5`](https://github.com/bec-project/bec/commit/c5f18e56a5f382a18ffc048907fea917f101bf75))

### Features

- 'bec_services_config_file_path' and 'bec_test_config_file_path' fixtures
  ([`d3f3071`](https://github.com/bec-project/bec/commit/d3f30712f957ecdaa5bd52d98e4acce060a8b1d9))

- **tests**: Fixtures for end-2-end tests (available as a pytest plugin)
  ([`b24f65a`](https://github.com/bec-project/bec/commit/b24f65a2a16f7048c9370ef576dede63da40e00e))

### Refactoring

- Renamed queueID to queue_id
  ([`996809f`](https://github.com/bec-project/bec/commit/996809f3a0915e3562c5d6f5cf9266b13508e6b6))

- Renamed scanID to scan_id
  ([`01b4e9c`](https://github.com/bec-project/bec/commit/01b4e9cc68efc3770a328e2165b69026186359c1))

- **CI**: End-to-end tests on the same host, with flushing and on multiple hosts, without flushing
  ([`57f4115`](https://github.com/bec-project/bec/commit/57f4115dfb24012ca49ab57d51ab0c1d94fe68c3))

Environment variables are used to parametrize dockerfile and scripts Introduce 'buildah' and
  'podman' instead of docker to build and run images. Simplification of Dockerfiles and CI scripts.

- **tests**: New 'subprocess_start' with 'no-tmux' option
  ([`b5ca2c3`](https://github.com/bec-project/bec/commit/b5ca2c30dd99fb8be5081b6a22fcd8f9b7b367bb))

### Testing

- **scan_bundler**: Refactored scan bundler tests and added thread check
  ([`8f29b44`](https://github.com/bec-project/bec/commit/8f29b44ddedda424664cc6a6753fbc533b767cef))


## v1.21.1 (2024-03-22)

### Bug Fixes

- Fixed cleanup execution
  ([`fd02675`](https://github.com/bec-project/bec/commit/fd02675256bd5160c8d658871019bff3705d60cd))

- **bec_client**: Report cleanup
  ([`e7e5413`](https://github.com/bec-project/bec/commit/e7e5413ac4e0e6f829797c314f558a26accc78be))

- **scan_server**: Shut down scan if scan worker is stopped
  ([`9e626a0`](https://github.com/bec-project/bec/commit/9e626a0c2fd15e77259fc8a1c32fc97892e5a830))

### Refactoring

- **bec_client**: Fixed formatter
  ([`53070d9`](https://github.com/bec-project/bec/commit/53070d94d7063abf9bdc6e7ca154728be5a0a95f))

- **scan_bundler**: Reduced logger level for msgs without scanID
  ([`e6c7098`](https://github.com/bec-project/bec/commit/e6c7098f7da52a6b2b39a3a19a4694b1f8ef374b))


## v1.21.0 (2024-03-21)

### Bug Fixes

- Validate endpoint for .get_last (#236), enhance endpoint validation
  ([`da5df48`](https://github.com/bec-project/bec/commit/da5df48008216cd609d47bebbf9a6f90b050ba53))

### Features

- Add "count" keyword arg to stream connector .get_last(), to retrieve last "count" items
  ([`e281b6a`](https://github.com/bec-project/bec/commit/e281b6aaa10567538476e71fa307fc266216ef9a))

count=1 by default


## v1.20.6 (2024-03-20)

### Bug Fixes

- Get username using standard "getpass" module
  ([`06b4afa`](https://github.com/bec-project/bec/commit/06b4afae4d2979e0bddc22d185d272b1f232c548))


## v1.20.5 (2024-03-20)

### Bug Fixes

- **bec_lib**: Fixed error propagation after client refactoring
  ([`eb5774a`](https://github.com/bec-project/bec/commit/eb5774a5da157834ff0a4f0e1e03ac6b7237267d))

### Testing

- **bec_lib**: Fixed tests for endpoint structure
  ([`6270107`](https://github.com/bec-project/bec/commit/62701077bd0cfe19762cb9eca2fc95d7d53c6609))


## v1.20.4 (2024-03-20)

### Bug Fixes

- **device_server**: Fixed readback for automonitor
  ([`7a3e1c2`](https://github.com/bec-project/bec/commit/7a3e1c21ee200484942ad0eafe748640ebeaf1f8))


## v1.20.3 (2024-03-20)

### Bug Fixes

- **bec_lib**: Fixed client shutdown for failed inits
  ([`fc8ff9b`](https://github.com/bec-project/bec/commit/fc8ff9bd7508119c303f5c589e05aab0ade17d77))


## v1.20.2 (2024-03-20)

### Bug Fixes

- **scan_server**: Fixed queue update
  ([`926f028`](https://github.com/bec-project/bec/commit/926f0287be5dffe63c9e318b19908431abbc54de))

- **scan_server**: Improved shutdown procedure
  ([`2417eb5`](https://github.com/bec-project/bec/commit/2417eb5e321944b10ef2568135828121f0023537))

### Refactoring

- Cleanup of BECClient
  ([`d1834a1`](https://github.com/bec-project/bec/commit/d1834a13c121432f08367775f17bd84498b9cb1a))

### Testing

- Reduced time for flyer sim
  ([`70b4b55`](https://github.com/bec-project/bec/commit/70b4b55c31f8b3d434dcf05013f0424d8571e5ae))


## v1.20.1 (2024-03-20)

### Bug Fixes

- **bec_lib**: Added queue update
  ([`1ee251d`](https://github.com/bec-project/bec/commit/1ee251d6e75fd84c9644e8cdc7b61ba99d0d155c))

- **bec_lib**: Fixed return value for put; closes #234
  ([`9109177`](https://github.com/bec-project/bec/commit/9109177a2e395eefb87f96750d3068e6936b2a25))


## v1.20.0 (2024-03-20)

### Documentation

- Added reference to bec-server attach
  ([`b92d757`](https://github.com/bec-project/bec/commit/b92d757c059f457a81a0324ca079f757f5e03d3b))

### Features

- **bec_server**: Added cli option to attach to the tmux session
  ([`5115c31`](https://github.com/bec-project/bec/commit/5115c316763c5674fd438ee0702e8f42c2f92109))


## v1.19.0 (2024-03-19)

### Features

- **device_server**: Added subscription to all auto monitored signals
  ([`816da5c`](https://github.com/bec-project/bec/commit/816da5cbc673b788973bd302fbf55cfd787e1c50))


## v1.18.1 (2024-03-18)

### Bug Fixes

- Unified access to limits; closes #233
  ([`648b720`](https://github.com/bec-project/bec/commit/648b720a9828c7ed6d7ac3c3750b80d18b8d0e24))

- **bec_lib**: Added pyepics compliant wait function to put
  ([`eb15e3a`](https://github.com/bec-project/bec/commit/eb15e3ae493d5046c20965846c702f6acfb055dc))


## v1.18.0 (2024-03-18)

### Features

- **device_server**: Simplified access to ophyd objects in the device server
  ([`9af29e4`](https://github.com/bec-project/bec/commit/9af29e48668d91a1e79da6c0c70608a24ad1cddc))


## v1.17.0 (2024-03-15)

### Bug Fixes

- **bec_client**: Fixed gui shutdown procedure
  ([`15649ac`](https://github.com/bec-project/bec/commit/15649acc5eae4a45b20ec8f2039c4d4f32bc41fd))

### Features

- **bec_client**: Becfigure start automatically on startup
  ([`9092122`](https://github.com/bec-project/bec/commit/9092122e67bf78b62d4d2afe4fdeaa2fd154495f))


## v1.16.1 (2024-03-15)

### Bug Fixes

- Fix scan_export and unit tests
  ([`914b332`](https://github.com/bec-project/bec/commit/914b332781683b2c738b3693341cebdcb799393e))

- **scan_bundler**: Allow missing entries in scan info
  ([`7349545`](https://github.com/bec-project/bec/commit/7349545ee056da9ec226dc6be9dff12748ce2066))

- **scan_segments**: Segments should not include the entire scan status
  ([`46ae12c`](https://github.com/bec-project/bec/commit/46ae12ccbe9c643f42a6d90014e0f37ecd16589d))

### Testing

- Fix formatting
  ([`14a0088`](https://github.com/bec-project/bec/commit/14a0088dea830e82cae551d56c7c06aa5aa18eda))


## v1.16.0 (2024-03-15)

### Bug Fixes

- **bec_lib**: Fixed support in dap for scan reports
  ([`0666013`](https://github.com/bec-project/bec/commit/0666013d909fa72f500179f0ce6926588bed9249))

### Features

- **bec_lib**: Added endpoint for gui heartbeats
  ([`01066dd`](https://github.com/bec-project/bec/commit/01066dd777e5800f69709e9f76fc192bd9a75a25))

- **device_server**: Added guards against using protected methods; closes #228
  ([`67e8eeb`](https://github.com/bec-project/bec/commit/67e8eeb255fabf44ed00605f056066b111791d2c))


## v1.15.0 (2024-03-15)

### Features

- **bec_lib**: Added started flag to bec client
  ([`e8eba90`](https://github.com/bec-project/bec/commit/e8eba9049ae33b5b83615ced8d2526dde54a5c7d))


## v1.14.6 (2024-03-15)

### Bug Fixes

- **bec_lib**: Device.describe should not be an rpc method
  ([`42fae6a`](https://github.com/bec-project/bec/commit/42fae6a70baa01565599899437a3bdca1d4783ee))

### Documentation

- Added bec_plugins link to developer.md
  ([`911192d`](https://github.com/bec-project/bec/commit/911192d566255dcf19e5c80442baeddb13e26908))

### Refactoring

- **bec_lib**: Minor cleanup of device status
  ([`1c6662c`](https://github.com/bec-project/bec/commit/1c6662cfe3f67884bc3bb073d34ca56077a00a0f))

### Testing

- **bec_lib**: Minor cleanup
  ([`6316f4f`](https://github.com/bec-project/bec/commit/6316f4f762e7f5665df7cb7581c7e73a4b0e4311))


## v1.14.5 (2024-03-14)

### Bug Fixes

- **bec_lib**: Fixed status timeout
  ([`c4f0a18`](https://github.com/bec-project/bec/commit/c4f0a18e7317caf54e95e7c7b1f09bf033e65380))

### Documentation

- **bec_lib**: Added module docs
  ([`7031c24`](https://github.com/bec-project/bec/commit/7031c2483163eb3268760b5ecd4888c9c5b6b372))

- **bec_lib**: Improved doc string for device module
  ([`c605846`](https://github.com/bec-project/bec/commit/c605846adeee080dbdecd55d5f758e2acd884d83))

### Testing

- **scan_server**: Fixed thread leak in scan server tests
  ([`fe69b6e`](https://github.com/bec-project/bec/commit/fe69b6e11cee239fc9799f135d83268761c3b6a8))


## v1.14.4 (2024-03-12)

### Bug Fixes

- **bec_lib**: Added tab complete for property vars
  ([`ef531d0`](https://github.com/bec-project/bec/commit/ef531d0d4a1848ac5917b56eebea385fac9b7a4c))

- **bec_lib**: Don't call rpc on jedi completer
  ([`8a6a968`](https://github.com/bec-project/bec/commit/8a6a968cc8c31fbb5fb20ce872b8bbdc76039ee8))

### Documentation

- Updated readme
  ([`c490574`](https://github.com/bec-project/bec/commit/c490574b9bafc217bf29bec9b087fa75c50abab6))

### Refactoring

- **bec_lib**: Fixed type hint
  ([`8c8dffb`](https://github.com/bec-project/bec/commit/8c8dffb5a6c1627cc2b02943e9ece4846db087ca))


## v1.14.3 (2024-03-12)

### Bug Fixes

- **bec_lib**: Fixed dataset number setter
  ([`5dcffe0`](https://github.com/bec-project/bec/commit/5dcffe022c2a5d2c1e2cb50265f5d0b1cefe547a))


## v1.14.2 (2024-03-12)

### Bug Fixes

- Add recovery_config files to .gitignore
  ([`6201757`](https://github.com/bec-project/bec/commit/62017574baea74dfff5f4d13ea1d1886ee6581a8))

- Remove debug prints from livetable
  ([`7efb387`](https://github.com/bec-project/bec/commit/7efb3878d8687ba4e747c99e517d4c6df40c6965))


## v1.14.1 (2024-03-11)

### Bug Fixes

- **scan_server**: Added cm for preventing race conditions within queue updates
  ([`b98dd52`](https://github.com/bec-project/bec/commit/b98dd52d6ac4bce23c0916028810340e1af74649))

### Continuous Integration

- Removed 'allow_failure' flag from bec-widgets
  ([`ad5e101`](https://github.com/bec-project/bec/commit/ad5e101bb6961a9c83bc8b31e1d91daf91c71197))

### Testing

- Cache test config
  ([`ec33aa5`](https://github.com/bec-project/bec/commit/ec33aa5fdf48dc39db10b603dac67c018f44eacb))


## v1.14.0 (2024-03-10)

### Bug Fixes

- **bec_lib**: Fixed signal update
  ([`689e2d9`](https://github.com/bec-project/bec/commit/689e2d968c4967bcaf8d19e2756997898671bf79))

- **scihub**: Rejected config should raise
  ([`af2e4c5`](https://github.com/bec-project/bec/commit/af2e4c58e1143b39e58f5e4f292d66dfcd36123f))

### Continuous Integration

- Added pseudo signal to config
  ([`eeb83d3`](https://github.com/bec-project/bec/commit/eeb83d3ba04746019dbc0c36ec6c817a39b7d72f))

### Features

- Added support for computed signals
  ([`720d6e2`](https://github.com/bec-project/bec/commit/720d6e210df11071f0d5c30442e6e50e34833844))

### Testing

- Added e2e test for pseudo signal
  ([`8b85165`](https://github.com/bec-project/bec/commit/8b85165bc4afcb044048e3b239b4dc7465d2636b))

- **bec_lib**: Added tests for computed signal
  ([`286e05d`](https://github.com/bec-project/bec/commit/286e05d2b34e44be7f1a9f2baaec50d71098568f))


## v1.13.3 (2024-03-10)

### Bug Fixes

- **bec_lib**: Fixed bug that caused data to be modified when using xadd; closes #220
  ([`3dbb8a0`](https://github.com/bec-project/bec/commit/3dbb8a00a1439e3030d9406a223040ef99cb60a8))


## v1.13.2 (2024-03-10)

### Bug Fixes

- **bec_lib**: Daemonized connector threads
  ([`be1f3fd`](https://github.com/bec-project/bec/commit/be1f3fd140af8fdf6216f41b92e92ed9126d3791))

- **bec_lib**: Shutdown loguru
  ([`3f8d655`](https://github.com/bec-project/bec/commit/3f8d655b2e199268bd23746fd0fe96bc316fcb8c))


## v1.13.1 (2024-03-10)

### Bug Fixes

- **scan_server**: Fixed flomni init; added tests
  ([`a3ceac7`](https://github.com/bec-project/bec/commit/a3ceac7a95f593da23575aaf60693519d4789764))

### Testing

- Fixed compliance with fakeredis > 2.21.1
  ([`56c0d7a`](https://github.com/bec-project/bec/commit/56c0d7ad6ce42b715750daf136cc7fa74fa9c3d1))

- Fixed flomni fermat scan tests
  ([`8c53e5b`](https://github.com/bec-project/bec/commit/8c53e5b090d947261376565da130da1cd55ca3e6))

- Fixed test
  ([`04d8f1b`](https://github.com/bec-project/bec/commit/04d8f1b0d5102212a53c2ee7e47d6169209812cf))


## v1.13.0 (2024-03-10)

### Features

- Remove asyncio from BECIPythonclient to support jupyter notebook output for progressbar
  ([`f9c1e81`](https://github.com/bec-project/bec/commit/f9c1e818e0c1aa43947c6c82e052af3f162338fa))

### Refactoring

- Remove all pytest-asyncio and asyncio dependencies
  ([`7413841`](https://github.com/bec-project/bec/commit/741384130ff64cb995fe809aa7cc08af6fcf557d))

### Testing

- Remove asyncio from tests
  ([`1de701c`](https://github.com/bec-project/bec/commit/1de701cd9b5d60a11fff7b915ad23f69431c16ae))


## v1.12.9 (2024-03-06)

### Bug Fixes

- **bec_lib**: Added missing unsubscribe from streams
  ([`75cd651`](https://github.com/bec-project/bec/commit/75cd6512ea4e0b00ad320d16f4298ca9f79d8105))

- **bec_lib**: Fixed support for lists in redis stream subscriptions
  ([`d4b7b42`](https://github.com/bec-project/bec/commit/d4b7b42f4608464da417435350a0c73f7665a44f))

- **bec_lib**: Fixed support for redis streams
  ([`7578395`](https://github.com/bec-project/bec/commit/757839534c75bd3a8110256cdfd770f770a195e0))

### Continuous Integration

- Pylint cleanup
  ([`776f5cb`](https://github.com/bec-project/bec/commit/776f5cb8967df1210a3df35a450c85d9d63eb90c))

### Documentation

- **bec_lib**: Improved endpoint doc strings
  ([`656478f`](https://github.com/bec-project/bec/commit/656478f784440b91ada8fb1da1d3957276079765))

- **bec_lib**: Updated doc strings
  ([`969b0a0`](https://github.com/bec-project/bec/commit/969b0a02f842a8d9666277fa5c4cc98344b8b0f6))

### Refactoring

- **bec_lib**: Changed connector to use abstract methods
  ([`d35b992`](https://github.com/bec-project/bec/commit/d35b992262f6b7f95b60b46dba6412dc3368785b))

### Testing

- **bec_lib**: Added more tests for the redis connector
  ([`7ca93d7`](https://github.com/bec-project/bec/commit/7ca93d74121c753a33ef9a1170accb85bd8a2515))

- **bec_lib**: Minor cleanup
  ([`93c8ec6`](https://github.com/bec-project/bec/commit/93c8ec66f9984b603926b29b831dc6f62f2af44e))


## v1.12.8 (2024-03-06)

### Bug Fixes

- Account is now a variablemessage
  ([`79d57b5`](https://github.com/bec-project/bec/commit/79d57b509d20451c73a0442adf477ae4299e9dc6))

- Added backward compatibility for scan numbers and dataset numbers
  ([`9ff2278`](https://github.com/bec-project/bec/commit/9ff2278c38b9fcb671c5591f15fda9473155fbe3))

- Logbook is now using a credentialsmessage
  ([`b62960f`](https://github.com/bec-project/bec/commit/b62960f6e697b116ffc4f3c5fded0c6bcd9ea4e2))

- Pre-scan macros are now using a VariableMessage
  ([`4239576`](https://github.com/bec-project/bec/commit/4239576c94c44041f395261b42de1056de8c0d76))

- Scan_number and dataset_number is now a VariableMessage
  ([`f698605`](https://github.com/bec-project/bec/commit/f698605579766536f1ec1e653e9d5e3dfd44166e))

### Continuous Integration

- Drop python/3.9
  ([`d16268c`](https://github.com/bec-project/bec/commit/d16268ce81e67b9d6cb5e1f153d52d61366c2b80))

### Documentation

- Adress comments
  ([`d556f84`](https://github.com/bec-project/bec/commit/d556f842353e7ef335309fb42025d00de193b627))

- Fixed last comments
  ([`aadbb01`](https://github.com/bec-project/bec/commit/aadbb01a947ae5419dff7c18837ab8ea9b75d78c))

- Typo
  ([`4ac0bbc`](https://github.com/bec-project/bec/commit/4ac0bbca1631ed5226458307f7ad35d155617328))

- Update documentation on the simulation
  ([`0ec3dac`](https://github.com/bec-project/bec/commit/0ec3dac85fd03413c2cf79b5881e8dffbecd2877))

- Updated contributing.md
  ([`af2bd27`](https://github.com/bec-project/bec/commit/af2bd273d510f08f420f39ec519d1a97ac8a1cb1))

### Refactoring

- Endpoints return EndpointInfo object instead of string
  ([`a4adb64`](https://github.com/bec-project/bec/commit/a4adb64f5fb083e4d3c20fe30247ce3b480e68cb))

- Removed remaining loads/dumps
  ([`2fd1953`](https://github.com/bec-project/bec/commit/2fd1953a551bcb1a0868034b28c629a2add64790))

### Testing

- **scan_server**: Fixed threading-related issue that caused test to fail from time to time
  ([`ae07b9f`](https://github.com/bec-project/bec/commit/ae07b9fab67cbcdef88ced4cb14c6c546568936f))


## v1.12.7 (2024-03-04)

### Bug Fixes

- **scihub**: Fixed scibec upload for large scans
  ([`2b680ee`](https://github.com/bec-project/bec/commit/2b680eee1ea51c1875ef8e1fea9f3135a3e52899))

### Continuous Integration

- Fixed pylint check
  ([`7094092`](https://github.com/bec-project/bec/commit/7094092f130547b6f12ff3dbec95a1e4553cfca2))

- Removed flaky from ci pipeline
  ([`9b54ebb`](https://github.com/bec-project/bec/commit/9b54ebb3ac6eae7e29896644c9545385e9ac41e5))

### Refactoring

- **file_writer**: Cleanup
  ([`d0a04de`](https://github.com/bec-project/bec/commit/d0a04de641db149097aa4cc6015dd0492c1aac3e))

### Testing

- Replaced flaky with pytest-retry
  ([`c2c5d33`](https://github.com/bec-project/bec/commit/c2c5d33f04654c4d88b08b92567a862bc47577eb))


## v1.12.6 (2024-03-01)

### Bug Fixes

- Fix dap test, cleanup redudant config values
  ([`4f63fef`](https://github.com/bec-project/bec/commit/4f63fef18c512eadbf339629e9780b82d878ea37))

### Testing

- Add proxy to test config
  ([`cb26a2a`](https://github.com/bec-project/bec/commit/cb26a2a59211ad5a695593b0e7131a9d51c4f2ac))


## v1.12.5 (2024-03-01)

### Bug Fixes

- **scan_server**: Fixed queue pop for pending requests
  ([`14f94cd`](https://github.com/bec-project/bec/commit/14f94cd96071feb0885b010af7576532baea553e))


## v1.12.4 (2024-02-27)

### Bug Fixes

- **bec_lib**: Exclude disabled devices in device filters
  ([`388baae`](https://github.com/bec-project/bec/commit/388baae9f117120f4f3db29e0c0db03cbb78b54c))

### Testing

- **config**: Made disabled device a monitored device
  ([`3979a1e`](https://github.com/bec-project/bec/commit/3979a1e5e21b79f2171f831f5badfad3c1cb3209))


## v1.12.3 (2024-02-27)

### Bug Fixes

- **scan_server**: Stage should only include monitored, baseline and async devices
  ([`05a83bd`](https://github.com/bec-project/bec/commit/05a83bd4ac1fe898863f24bac1a139da0836a46a))


## v1.12.2 (2024-02-26)

### Bug Fixes

- **disconnection**: Mitigate effects on disconnection from redis
  ([`4d73cf8`](https://github.com/bec-project/bec/commit/4d73cf8a071493ec997ca08efc8518672c7f5034))

- **redis_connector**: Add producer(), consumer() for compatibility with old code
  ([`f60a012`](https://github.com/bec-project/bec/commit/f60a012ef6453742bb8c830e479325bfb9254b87))

With deprecation warnings

- **scan_manager**: Ensure robustness in __str__
  ([`db53b1f`](https://github.com/bec-project/bec/commit/db53b1f5dc73279c6764d1f4dd875f32304d1f5d))

- **tests**: Ensure redis is installed
  ([`bbd036e`](https://github.com/bec-project/bec/commit/bbd036e769bb6093d1890fceb23e005baa644888))


## v1.12.1 (2024-02-24)

### Bug Fixes

- **scan_server**: Fixed expected message type for device progress update
  ([`1236069`](https://github.com/bec-project/bec/commit/1236069b3604607288f9f0e1dccd3994d014f928))


## v1.12.0 (2024-02-24)

### Features

- Added flomni scan and user scripts
  ([`c376de8`](https://github.com/bec-project/bec/commit/c376de8e8436380f65ba96b2e88572077830f1d9))

### Refactoring

- Minor cleanup
  ([`b8f8467`](https://github.com/bec-project/bec/commit/b8f846749b8042e6c6ce6bcb1f05bc191da96f42))


## v1.11.1 (2024-02-23)

### Bug Fixes

- **file_writer**: Fixed data update
  ([`16f8f30`](https://github.com/bec-project/bec/commit/16f8f30ea6fc15173abbfccee65b869018659bca))

- **scan_bundler**: Fixed scan bundler update
  ([`2e5b147`](https://github.com/bec-project/bec/commit/2e5b147a2c9ccd6ca7169f45f0431ed1df902b0f))

- **scan_server**: Fixed inheritance for flyers
  ([`5f80220`](https://github.com/bec-project/bec/commit/5f80220fa2d062112dd5770b3485dd478ead63f8))

- **scan_server**: Reverted changes to monitor scan
  ([`636e060`](https://github.com/bec-project/bec/commit/636e0609f2d05ea079661872858084b3f9b3847e))

### Refactoring

- Renamed enforce_sync to monitor_sync
  ([`63a8dd8`](https://github.com/bec-project/bec/commit/63a8dd814c99296646c1429af6c31a5d625c5a8d))


## v1.11.0 (2024-02-23)

### Features

- Add Ophyd DeviceProxy to backend for simulation framework, delayed init of proxies
  ([`d37c5e7`](https://github.com/bec-project/bec/commit/d37c5e739120baad5ffef22888ba264a74663e63))


## v1.10.0 (2024-02-23)

### Bug Fixes

- **bec_lib**: Fix after refactoring
  ([`0337f13`](https://github.com/bec-project/bec/commit/0337f13d0453ca1fac77413d757746f2bc06eb95))

- **bec_lib**: Fixed bl_checks cleanup
  ([`05b00da`](https://github.com/bec-project/bec/commit/05b00dadbbabffc5efd32e9621336e335564db76))

- **bec_lib**: Fixed config helper for failed config updates
  ([`f603419`](https://github.com/bec-project/bec/commit/f6034191f5763495130cab25edc5d600d0da274d))

- **bec_lib**: Fixed service id assignment
  ([`3826d41`](https://github.com/bec-project/bec/commit/3826d410527177acce339e307a17c2943a921aa4))

- **bec_lib**: Fixed service init
  ([`b09b5ff`](https://github.com/bec-project/bec/commit/b09b5ff7cddd0d13c6c4a2d55d00914b131e59fd))

- **bec_lib**: Save guard device manager init
  ([`fcbc240`](https://github.com/bec-project/bec/commit/fcbc2402e437a756c11b09f5d1ce9a5351a0cc54))

- **scihub**: Added updated config flag to detect failed validations
  ([`3a133bf`](https://github.com/bec-project/bec/commit/3a133bf7d44a9578587e5bbee484183a21e9cc7c))

### Continuous Integration

- Added nightly e2e test
  ([`1fe6805`](https://github.com/bec-project/bec/commit/1fe680555098e960689ad423e0f2473807640d40))

### Features

- **bec_lib**: Added config history endpoint
  ([`ee7ecef`](https://github.com/bec-project/bec/commit/ee7ecef8d52320356f2190620bc3e42fa37db304))

- **bec_lib**: Report on failed devices; save recovery file to disk
  ([`8062503`](https://github.com/bec-project/bec/commit/806250300e472a780329e6438c1891565481d8f7))

- **device_server**: Connection errors and init errors are separated and forwarded
  ([`c2214b8`](https://github.com/bec-project/bec/commit/c2214b86468a2ac590ee3fd5eda19734dbac1c26))

- **scihub**: Added config reply handler for device_server updates
  ([`29a1d19`](https://github.com/bec-project/bec/commit/29a1d19504d6cbfb4a4601106d5341aadb0f43f7))

### Refactoring

- **bec_lib**: Minor cleanup
  ([`16b9e9c`](https://github.com/bec-project/bec/commit/16b9e9ca322f3d7474f9230d9b13c887ca20569c))

### Testing

- Fixed tests
  ([`e5aeb51`](https://github.com/bec-project/bec/commit/e5aeb510bfd0a477d817a45007a66b89f83bdd7f))

- **bec_lib**: Added config test for invalid device class
  ([`32df081`](https://github.com/bec-project/bec/commit/32df081906db6c80dfb572828bc06446b7134cba))

- **bec_lib**: Added tests for config updates; added threads check
  ([`67b96b9`](https://github.com/bec-project/bec/commit/67b96b9a6f96346e23f9b0eac9b3ac642e447d13))

- **scibec**: Fixed expected return values
  ([`79cd2ff`](https://github.com/bec-project/bec/commit/79cd2ff2f696713f405ce575bb8dbce26e49e8e8))


## v1.9.0 (2024-02-22)

### Bug Fixes

- **scihub**: Fixed data serialization before upload to scibec
  ([`eae1d61`](https://github.com/bec-project/bec/commit/eae1d617d6c44a933e7e86ea86e35d380d323f6a))

- **scihub**: Fixed error handling
  ([`fd3cb02`](https://github.com/bec-project/bec/commit/fd3cb025c50c61f3384d44abc8c234371952d731))

### Features

- **bec_lib**: Added json serializer
  ([`25366c0`](https://github.com/bec-project/bec/commit/25366c09638cf4e005ee4a75d64cf8f9eeba00ca))


## v1.8.0 (2024-02-20)

### Bug Fixes

- **bec_lib**: Fixed typo in xrange
  ([`0fe0a6e`](https://github.com/bec-project/bec/commit/0fe0a6e119ed01ebb38ac0b07c05d7f42a19ecf3))

### Continuous Integration

- Added all logs from client
  ([`da59e01`](https://github.com/bec-project/bec/commit/da59e01042a199f95506d568c13fa74866c29af0))

### Documentation

- **developer**: Added instructions on how to set up vscode
  ([`94d63e6`](https://github.com/bec-project/bec/commit/94d63e60b754a926e953208b325b943aa2c387e8))

### Features

- **bec_lib**: Added async data handler
  ([`da46c27`](https://github.com/bec-project/bec/commit/da46c278425fe31aa2a597c9d55065653ae63fbd))

### Refactoring

- **bec_lib**: Merged scan data and baseline data; added async data storage
  ([`06ed833`](https://github.com/bec-project/bec/commit/06ed833ab2ef338c3c443d127acb123e1744ed44))

- **file_writer**: Use async data handler
  ([`00d1fe0`](https://github.com/bec-project/bec/commit/00d1fe0afac64304e074d90b0e9893cf09021785))

### Testing

- **async_data**: Added more tests
  ([`fb7c84f`](https://github.com/bec-project/bec/commit/fb7c84fcc6b60f021684b3bd14e9ce22e5e3af72))

- **bec_lib**: Updated test data to new xrange return values
  ([`0b5660f`](https://github.com/bec-project/bec/commit/0b5660f1d41978dda6a8d37805e390fc292fd316))

- **file_writer**: Updated test data to new xrange return values
  ([`d5cfbb3`](https://github.com/bec-project/bec/commit/d5cfbb37538618fe2be5aaee10d9b0ac9122df7c))

- **pdf_writer**: Added tests
  ([`f64bdea`](https://github.com/bec-project/bec/commit/f64bdea7978c7ba5d98cb8b6933c91b54dd2865f))


## v1.7.3 (2024-02-19)

### Bug Fixes

- **rpc**: Fixed rpc calls with empty list as return value
  ([`a781369`](https://github.com/bec-project/bec/commit/a781369e469a1196b7e88c6fa3593ee40439ec1e))


## v1.7.2 (2024-02-19)

### Bug Fixes

- **endpoints**: Added gui_instruction_response endpoint
  ([`9a838bb`](https://github.com/bec-project/bec/commit/9a838bbe8e7fab974982654bdc9f69a14edf7a20))

- **live_table**: Fixed live table for string values
  ([`3b04d31`](https://github.com/bec-project/bec/commit/3b04d319f9f8d56a8e8c2d6fec9802cc7e747ad3))


## v1.7.1 (2024-02-16)

### Bug Fixes

- **bec_lib.device**: Made device.position a property to be compliant with ophyd
  ([`4060b86`](https://github.com/bec-project/bec/commit/4060b8651b99e3e86e440bcd02d148722ea5403b))

### Testing

- **e2e**: Added wait to set
  ([`27c53da`](https://github.com/bec-project/bec/commit/27c53dab774f68a2eec2c833f2b9316d2775ee51))


## v1.7.0 (2024-02-14)

### Bug Fixes

- **devicemanager**: Fixed bug after refactoring
  ([`37a58ef`](https://github.com/bec-project/bec/commit/37a58ef5ce83a1c1a4a483c07e08e4f5dd437dda))

### Features

- **devicemanager**: Added filter methods for continuous and on_request devices
  ([`708aaff`](https://github.com/bec-project/bec/commit/708aaff918e7e858c3c1070057a17cedab248c88))

- **scan_worker**: Emitted readout priority contains all devices, not just the modification
  ([`21187ad`](https://github.com/bec-project/bec/commit/21187adb48495422aa9c0f0adbeeaa23b2d6c8a5))

### Testing

- **devicemanager**: Updated test cases
  ([`ff17e50`](https://github.com/bec-project/bec/commit/ff17e508e6016163b9ddc32aa91f48a35de26b0b))


## v1.6.0 (2024-02-14)

### Features

- **file_writer**: Added support for user-defined file suffixes
  ([`505df05`](https://github.com/bec-project/bec/commit/505df053c1b5801cc86e5e6d1f3e58a94edeaf22))

### Testing

- Added tests for file suffix checks
  ([`7dc6c12`](https://github.com/bec-project/bec/commit/7dc6c12872546656a22f05f5c8901cf4ca07d42e))

- Refactored scan object tests
  ([`989aea4`](https://github.com/bec-project/bec/commit/989aea4531da77025a3b8a8c837ba5e568cf77bd))


## v1.5.1 (2024-02-14)

### Bug Fixes

- **dap**: Dap service should now raise on unknown dap service cls; another provider may be
  responsible for it
  ([`85969f5`](https://github.com/bec-project/bec/commit/85969f5d5393166996dbabbeefc06ef464576ee5))


## v1.5.0 (2024-02-13)

### Bug Fixes

- **bluesky_emitter**: Fixed device info access
  ([`06b2373`](https://github.com/bec-project/bec/commit/06b2373fea588433026079ed3d0a5b140d3f57f9))

- **scan_bundler**: Added metadata to baseline reading
  ([`9294562`](https://github.com/bec-project/bec/commit/9294562f3bb377531f2fc558a655e8f7016feb38))

- **scan_manager**: Added baseline consumer
  ([`ce9681c`](https://github.com/bec-project/bec/commit/ce9681c1859fd56c82bab206c8886f2851e4cc5d))

### Features

- **scan_data**: Added baseline data
  ([`41bf2d1`](https://github.com/bec-project/bec/commit/41bf2d1bd4d139f801d36d8cb4a0f9c837ac5d09))

### Refactoring

- **scan_data**: Removed unused variable
  ([`ef07a32`](https://github.com/bec-project/bec/commit/ef07a328bd05cd18e107229f56639a7492f3ba84))


## v1.4.0 (2024-02-13)

### Bug Fixes

- **dap**: Output fit is fitted to full scope, not only the trimmed
  ([`58f3cb1`](https://github.com/bec-project/bec/commit/58f3cb17209acbae58720bcb1d6e8ec3bac28162))

### Build System

- **plugins**: Added device server
  ([`3139ea0`](https://github.com/bec-project/bec/commit/3139ea09906c3d4569d8043fb05b1c21e290aeb6))

- **plugins**: Added helper script to create default plugin structure
  ([`04dc5bf`](https://github.com/bec-project/bec/commit/04dc5bfb66a7600c7c8de32cb7acbac9fd3b32f0))

### Continuous Integration

- Added pipeline as trigger for e2e tests
  ([`766795a`](https://github.com/bec-project/bec/commit/766795a91e5a3b3a4820a08ade4bc53d120b7105))

### Documentation

- **plugins**: Added plugin docs
  ([`c14c526`](https://github.com/bec-project/bec/commit/c14c526a85a9e67e1bca866fcae749975acc5f30))

### Features

- Added support for customized lmfit params
  ([`f175fd7`](https://github.com/bec-project/bec/commit/f175fd7c55b82dbb0d8b080ee8a93cd8be52d816))

### Refactoring

- Add dap_services, renamed ophyd_devices to devices and hli to high_level_interface
  ([`0c33ba7`](https://github.com/bec-project/bec/commit/0c33ba7616d170748a79aae9405e841b76097adc))

- Fix typos and structure
  ([`4dd6263`](https://github.com/bec-project/bec/commit/4dd6263c649afc8ea7a3faf33de2722152923156))

### Testing

- **dap**: Fixed return value
  ([`7aa90b6`](https://github.com/bec-project/bec/commit/7aa90b6c01324126e847849277a8e69efe5caceb))


## v1.3.0 (2024-02-12)

### Bug Fixes

- **dap**: Added input into to the dap metadata
  ([`0156f61`](https://github.com/bec-project/bec/commit/0156f611f110f5ae31e163b9630d19a4112e98d0))

### Build System

- **client**: Added flaky and pytest timeout dependencies
  ([`08103f5`](https://github.com/bec-project/bec/commit/08103f5fd2674a2cdf55d96ce3653c9802dc2de2))

### Continuous Integration

- Added flaky
  ([`06fb8bf`](https://github.com/bec-project/bec/commit/06fb8bfaf943d82b7492e755892c583701d33ab9))

### Documentation

- Fixed typo ophyd.md
  ([`c1f4b38`](https://github.com/bec-project/bec/commit/c1f4b388032689cf4f2d9492f6e49d32851b6035))

- **dap**: Added data fitting to user docs
  ([`b881b08`](https://github.com/bec-project/bec/commit/b881b081726c659056c5120e0a71e09d41ef8881))

- **ophyd**: Fixed ophyd config description
  ([`6a272de`](https://github.com/bec-project/bec/commit/6a272dede1d23ac7af9bb158105b2d5eb2c7445b))

### Features

- Added support for customized dap plugins objects
  ([`cf8430f`](https://github.com/bec-project/bec/commit/cf8430f8787c8028d518c17fea5f00aba6e94093))

- **dap**: Added input_data and plot options
  ([`c4029a0`](https://github.com/bec-project/bec/commit/c4029a098db66baf1741bf4bcc1bbd71d14aca7f))

- **DAP**: Added option to customize results based on the dap service
  ([`dd48519`](https://github.com/bec-project/bec/commit/dd4851941c14b1ace0227ab3ff28b50826c22f89))

- **dap**: Added option to filter x range before fitting data
  ([`7068c1f`](https://github.com/bec-project/bec/commit/7068c1f738d9876ef13cb62d797ba15fada3bd98))

### Refactoring

- Added str method to message objects
  ([`f9b2813`](https://github.com/bec-project/bec/commit/f9b2813a1deda03d5c1061fcbe7a20442405e056))

- Removed outdated worker manager
  ([`ba74299`](https://github.com/bec-project/bec/commit/ba74299a1dbd3aa3fd8ba38134889eb00df03142))

### Testing

- Added more tests for bec scans
  ([`101bc1f`](https://github.com/bec-project/bec/commit/101bc1ff1578e54b0dbd5b11ea3a92e7c32e5a96))

- Fixed import for dap plugins
  ([`b119e46`](https://github.com/bec-project/bec/commit/b119e464d6d3f6b325ec3dd53454bab7ff10a1fe))

- Fixed test after modifying dap interface
  ([`d57e95e`](https://github.com/bec-project/bec/commit/d57e95e20d2951746c86a51a1f2d307a4b7d2448))

- Fixed test as get data returns a np array instead of list
  ([`867773b`](https://github.com/bec-project/bec/commit/867773be376c0e7c43890337b4f86ceace8b4472))

- Marked mv as flaky
  ([`496442a`](https://github.com/bec-project/bec/commit/496442a6c8b1009c5f152a5863dc17a7cfc9a3c6))

- Use dev dependencies instead of hard-coded deps in Dockerfile
  ([`53bb5a6`](https://github.com/bec-project/bec/commit/53bb5a61290da65791525d4bf6f7c11998eee90b))


## v1.2.1 (2024-02-10)

### Bug Fixes

- **compat python 3.11**: Ensure "kind" test works for numbers too
  ([`697ae59`](https://github.com/bec-project/bec/commit/697ae59a6671aba27c098460e0d4ab59de62187d))

### Refactoring

- Clean up SimMonitor related changes
  ([`782dea8`](https://github.com/bec-project/bec/commit/782dea87f303a9a25fa98496dbf9710dfba337e5))

- **configs**: Cleanup configs and removed old SynDevices, exchanged with
  SimCamera/SimMonitor/SimPositioner
  ([`3293925`](https://github.com/bec-project/bec/commit/3293925256b1c7e1f9c99d93d05a2047ececb91f))


## v1.2.0 (2024-02-09)

### Features

- **bec_lib.utils**: Add user_access decorator to bec_lib.utils.
  ([`0b309ce`](https://github.com/bec-project/bec/commit/0b309ce8887b40cf907b19421adbbd47f30a8207))

refactor: add check if method is in USER_ACCESS already

test: add test for decorator

### Refactoring

- Cleanup
  ([`5b46810`](https://github.com/bec-project/bec/commit/5b46810270f722f4844a4d88136f41f6ebdeebe4))


## v1.1.3 (2024-02-09)

### Bug Fixes

- **serializer**: Fixed serialization for set
  ([`bcd2e06`](https://github.com/bec-project/bec/commit/bcd2e06449923b0f2f92f64703a70e6de99e72d2))

### Continuous Integration

- Added dap to e2e tests
  ([`6f4fb10`](https://github.com/bec-project/bec/commit/6f4fb10a28d8b47f1f29ac53d43ecc107a58e316))

- Added missing docker file to build dap
  ([`950504c`](https://github.com/bec-project/bec/commit/950504c72836ed192d29c6fdb93169b08c6f8e52))

### Refactoring

- **serializer**: Cleanup
  ([`211e614`](https://github.com/bec-project/bec/commit/211e614ed3032202b5684431d3855d8c82606ef1))

### Testing

- Added e2e test for dap and sim fit
  ([`7f257a9`](https://github.com/bec-project/bec/commit/7f257a929c486d0280c91ea8351a015a63b5e232))

- Fixed sim type for dap test
  ([`7e9da19`](https://github.com/bec-project/bec/commit/7e9da194459acded34924331c09c92977c4b2363))


## v1.1.2 (2024-02-09)

### Bug Fixes

- Fixed init config script
  ([`f0f9fe3`](https://github.com/bec-project/bec/commit/f0f9fe304f24cb61053ce12d785b82745a14210e))

- Fixed xread decoding
  ([`c684a76`](https://github.com/bec-project/bec/commit/c684a76a5eff0bf478121dcde25e9e4447e839dc))

- Removed outdated msgpack packing
  ([`15f8e21`](https://github.com/bec-project/bec/commit/15f8e213053a635da4ed8d54bcb562a690e71517))

fix: fixed return values of configs

- **redis_connector**: Encode/decode stream data
  ([`a7bafa6`](https://github.com/bec-project/bec/commit/a7bafa6a0c99395068ef090d149732b4fa4f5eb5))

- **serialization**: Move BECMessage check in .loads() in the BEC message decoder
  ([`de8333b`](https://github.com/bec-project/bec/commit/de8333b164033ff0c0a300278e5494f5cc8f7879))

This allows to call .loads() on any bytes string


## v1.1.1 (2024-02-08)

### Bug Fixes

- Bugfix for .put and cached readback,
  ([`880eb77`](https://github.com/bec-project/bec/commit/880eb77fd0a065cadde14bc33294b069d16fd0c6))

### Refactoring

- Simplified updated logic
  ([`75e06d3`](https://github.com/bec-project/bec/commit/75e06d3ee3296d636bd88e71ad519bcd6989d29e))

### Testing

- Add modified test including .put from client
  ([`a8204ac`](https://github.com/bec-project/bec/commit/a8204ac84820562374d335cbbb5584ad14d879af))


## v1.1.0 (2024-02-08)

### Bug Fixes

- Accept args, kwargs in new client
  ([`47f4ecf`](https://github.com/bec-project/bec/commit/47f4ecff540e03004ad16afaa6d11150efd0d3ba))

- Fixed dap loading
  ([`0c71b5f`](https://github.com/bec-project/bec/commit/0c71b5f7c2c7d198ced36de9ced40bf99287025a))

- Fixed dataclass for availableresourcemessage
  ([`cfc5237`](https://github.com/bec-project/bec/commit/cfc5237025806d91eb32f2ebe135e2ddb55e8c1a))

- Fixed multiple model imports
  ([`a5f1447`](https://github.com/bec-project/bec/commit/a5f1447c8734f88734f9828753f7bfd567128b8e))

- Fixed plugin handling for multiple service provider
  ([`ef4b66f`](https://github.com/bec-project/bec/commit/ef4b66fd928600e40c06880cba807e667b1b4ed2))

- Fixed scan_data for .get default values
  ([`aa403c4`](https://github.com/bec-project/bec/commit/aa403c4a53a7c2462d623097a2f3275f2e7c1c90))

- Fixed support for multiple dap services
  ([`e2595bc`](https://github.com/bec-project/bec/commit/e2595bc22535de384d2747bbb5d4c225507f2a11))

- Fixed support for scan items as args
  ([`b15e38b`](https://github.com/bec-project/bec/commit/b15e38bd39511e09ed4e456843e54e755a1c10ce))

- Fixed typo
  ([`4c8dd55`](https://github.com/bec-project/bec/commit/4c8dd5570cde952a24b60ac4a9eb6227d2b5d364))

- Fixed xread
  ([`f1219ae`](https://github.com/bec-project/bec/commit/f1219ae0f051a872bdbb6e5d5e68c8e8b16232ea))

- Fixed xread when no id is specified
  ([`4a42277`](https://github.com/bec-project/bec/commit/4a4227731ffeb117f4a5b8fb5f3ce6045f1f8eb8))

- Improved error handling for scihub
  ([`5d8028d`](https://github.com/bec-project/bec/commit/5d8028db6bb9c510a8738ddba1545797b07dd7eb))

- Preliminary fix for missing xadd serialization
  ([`11b8326`](https://github.com/bec-project/bec/commit/11b8326393e0b21f0f2ad140c826af18619193f0))

- Use service id property for status updates
  ([`5d33146`](https://github.com/bec-project/bec/commit/5d33146b0cf7e6f9f23f98275e2697459d3692dd))

### Build System

- Added lmfit dependency
  ([`c90f24f`](https://github.com/bec-project/bec/commit/c90f24fa644875866255bba0abbed96f1b783d23))

### Features

- Added dap endpoints and messages
  ([`6dc09cf`](https://github.com/bec-project/bec/commit/6dc09cf6e5ea26f8b64e266776322cbee19550bc))

- Dap services
  ([`2b08662`](https://github.com/bec-project/bec/commit/2b08662c08baf89ecbbd051a428914b78bbbcd97))

- Lmfit serializer
  ([`ed679bb`](https://github.com/bec-project/bec/commit/ed679bb6650dcd7d77ebbe16212745ac985eeba8))

### Refactoring

- Added private method to simplify the on_scan_status update routine
  ([`8b223a1`](https://github.com/bec-project/bec/commit/8b223a1e914f4af2af9e0ed49dd609ad2dce5163))

- Cleanup
  ([`9d1a494`](https://github.com/bec-project/bec/commit/9d1a494d9d19cfd50c2a92954c6eb1213a8af3f0))

- Cleanup after rebasing
  ([`514a516`](https://github.com/bec-project/bec/commit/514a51624bd94fb4af55fcedba37bb155db29dc1))

- Cleanup and formatting
  ([`9a98dba`](https://github.com/bec-project/bec/commit/9a98dbafd4f91189d5a8a5a97bb45568bbdedace))

- Removed outdated dap files
  ([`bd2a4dd`](https://github.com/bec-project/bec/commit/bd2a4ddd75d6d16dc22d16a50a408b85c7119b05))

- Renamed fit to run and added service property to override default method name
  ([`d510ae2`](https://github.com/bec-project/bec/commit/d510ae2b61472d258f887c8c8ea4d3c653d5a723))

test: fixed tests after renaming fit to run

test: fixed test after refactoring

test: added more tests

### Testing

- Added tests for dap plugins
  ([`b09524e`](https://github.com/bec-project/bec/commit/b09524eb0b1a2686e2d377f9836139a948c25b8d))

- More tests
  ([`aa9ff2d`](https://github.com/bec-project/bec/commit/aa9ff2d3d0fdf0db102748a9c5968e969824b34f))


## v1.0.0 (2024-02-07)

### Bug Fixes

- **AlarmMessage**: Content member (dict) changed to msg (str)
  ([`3d087ef`](https://github.com/bec-project/bec/commit/3d087ef87d1480ff4c96873e658441a833f477c6))

In practice it was already used as a message.

- **global var**: Remove builtins.__BEC_SERVICE__
  ([`80fddc5`](https://github.com/bec-project/bec/commit/80fddc5a4c3ec16d0ac464c79dcfba6bf449242e))

- **messages**: Set msg_type of ScanQueueMessage to "scan_queue_message"
  ([`8b125a0`](https://github.com/bec-project/bec/commit/8b125a05b95084c676552c483b4d35eaa45724a2))

This allows 'shortcut lookup' for message class

- **scan_manager**: Publish available scans using a BECMessage
  ([`ac5fafc`](https://github.com/bec-project/bec/commit/ac5fafc7fa30dc8006edb7554c9e261c30edc9a0))

### Refactoring

- **connector**: Add message type check in "send", use "raw_send" to publish arbitrary bytes
  ([`d4c4cba`](https://github.com/bec-project/bec/commit/d4c4cbaaf26644d33c8e3685aef498e9db42790a))

- **messages**: Messages refactoring, new serialization module
  ([`8bbfd10`](https://github.com/bec-project/bec/commit/8bbfd10ca7db4b8376478a421633fe3e94cd9f0e))

To have a better separation of concern between messages and how they are conveyed in connectors.
  BECMessage can be simple dataclasses, leaving the serialization to the connector which transport
  those. The serialization module itself can be isolated, and in the case of msgpack it can be
  extended to understand how to encode/decode BECMessage, to simplify writing code like with
  BundleMessage or to be able to automatically encode numpy or BECStatus objects. Finally, client
  objects (producers and consumers) can receive BECMessage objects instead of having to dump or load
  themselves.


## v0.61.0 (2024-02-06)

### Bug Fixes

- **flake8**: Apply flake8 to fix inconsistencies
  ([`2eafba9`](https://github.com/bec-project/bec/commit/2eafba951ae7180131bebf705466cdb2968bddba))

### Features

- **ipython**: Represent objects using '__str__' rather than '__repr__'
  ([`1e2af9a`](https://github.com/bec-project/bec/commit/1e2af9ae519382fe490162dff60b1056b9f50fdf))


## v0.60.3 (2024-02-05)

### Bug Fixes

- Scan_to_csv can handle ScanReport and ScanItem, runs on multiple scans; closes #80, #104
  ([`1ff19a1`](https://github.com/bec-project/bec/commit/1ff19a156d9614fb1d9583c28d8181341ad5ebb4))

### Continuous Integration

- Added development tag update
  ([`05c4328`](https://github.com/bec-project/bec/commit/05c432852cb976e85e7f18a0ee5b997645883d0a))

- Added rtd deployment to dev on MR
  ([`b897c4f`](https://github.com/bec-project/bec/commit/b897c4fd0509104223f7d60344b49728e7f80cc9))

- Changed job order for dev rtd deployment
  ([`a7afda9`](https://github.com/bec-project/bec/commit/a7afda9d789010f8e800910e687635d58aaf9ed8))

- Dockerfile accepts build var for python version
  ([`7b5391a`](https://github.com/bec-project/bec/commit/7b5391a4f01a8acfcdc9c4eda7ef6065685d9dd3))

- Excluded dev branch from pipeline
  ([`9a7e817`](https://github.com/bec-project/bec/commit/9a7e817e541a599dcee06ef2be7b93329f8f3632))

- Moved branch exclusion to job
  ([`d8381b9`](https://github.com/bec-project/bec/commit/d8381b9850c6d77a6f1d3cb55bcbf31a266b79fc))

- Removed no-ci
  ([`c696317`](https://github.com/bec-project/bec/commit/c696317f68ae0fd7f0130a800ad570b29af3b8f2))

- Reverted changes to .gitlab-ci.yml
  ([`22261ad`](https://github.com/bec-project/bec/commit/22261ad411c6d6f0b1cef201021656fba08072b6))

- Updated .gitlab-ci.yml
  ([`f862074`](https://github.com/bec-project/bec/commit/f8620748646febc2df195fa32664795ec1a56c27))

- Updated .gitlab-ci.yml
  ([`22a1995`](https://github.com/bec-project/bec/commit/22a19958e71fa8ecd44a4b99626b4b406f747031))

- Updated .gitlab-ci.yml file
  ([`7edf969`](https://github.com/bec-project/bec/commit/7edf96964953f999b83797662f9534af4439325e))

### Documentation

- Added references to the user guide
  ([`a25c4b2`](https://github.com/bec-project/bec/commit/a25c4b2bb0ef7d98ef72c0bfefd6b435d4bb1db0))

- Complement documentation
  ([`b4ac84a`](https://github.com/bec-project/bec/commit/b4ac84a960db767eed0d6686ab242d52e7c3ac06))

- Fixed docstrings
  ([`f1e8662`](https://github.com/bec-project/bec/commit/f1e86627773659b76b8cff199709611d23a0c558))

- Updated landing page readme
  ([`c30eb2e`](https://github.com/bec-project/bec/commit/c30eb2e67e89542d87a7aef8d7e445165f827239))


## v0.60.2 (2024-02-02)

### Bug Fixes

- Fixed scihub shutdown procedure
  ([`dfc6dd4`](https://github.com/bec-project/bec/commit/dfc6dd4aaba1d5a9fcda51b2e8d30e9a431f237f))

### Refactoring

- Cleanup
  ([`afe8e24`](https://github.com/bec-project/bec/commit/afe8e24f40ab53516311cddf9a4113090cff3534))

### Testing

- Added scilog tests
  ([`f8bf994`](https://github.com/bec-project/bec/commit/f8bf9943b18d8e94c7113862e987d7dd3be8764c))

- Added shutdown methods to consumer mocks
  ([`4d827fe`](https://github.com/bec-project/bec/commit/4d827fe16c982684fcca2e63f6bad1e486215309))

- Allow for some jitter
  ([`d560432`](https://github.com/bec-project/bec/commit/d5604323b6a33f86ecaae2e66a2cba0802cdd469))


## v0.60.1 (2024-02-02)

### Bug Fixes

- Fixed serializer for 3.9
  ([`5c6f250`](https://github.com/bec-project/bec/commit/5c6f250950438deccfe61dadaf6f2224ebae6243))

- Fixed signature serializer for union operator; cleanup
  ([`4dd682b`](https://github.com/bec-project/bec/commit/4dd682b3e7aaae1c40ed775554496c674f44658e))

### Refactoring

- Cleanup
  ([`1f802a0`](https://github.com/bec-project/bec/commit/1f802a08bbd24d7f23b0481b82bd6ed07e665758))


## v0.60.0 (2024-02-01)

### Features

- **run environment**: Allow to run service using the current Conda environment, if any
  ([`4b3bb4a`](https://github.com/bec-project/bec/commit/4b3bb4a2a87d2d3024637500ad00e13dca80cac2))


## v0.59.6 (2024-02-01)

### Bug Fixes

- Fixed scibec login update
  ([`f1d8faf`](https://github.com/bec-project/bec/commit/f1d8fafeaf3fd961fb4e3fa07a845a037fc27d1b))


## v0.59.5 (2024-01-31)

### Bug Fixes

- Fixed get_software_triggered_devices to excluded disabled devices, complement test case
  ([`37e74dc`](https://github.com/bec-project/bec/commit/37e74dc206e906a15d19a261fc768b86d70cfdc1))


## v0.59.4 (2024-01-31)

### Bug Fixes

- Fixed event name for scan status callbacks
  ([`ed43260`](https://github.com/bec-project/bec/commit/ed43260b60297d4e6b8dddc8c853b53b653c9ce1))

### Testing

- Fixed test for scan status events
  ([`ce67a29`](https://github.com/bec-project/bec/commit/ce67a291eb188b40dc4259e611608417c3bdcd9e))


## v0.59.3 (2024-01-30)

### Bug Fixes

- Fixed rpc calls on device properties
  ([`f778302`](https://github.com/bec-project/bec/commit/f77830296605e64c66b9ae3c8c9d760db720fe23))


## v0.59.2 (2024-01-30)

### Bug Fixes

- Added put as trigger for an update of the config cache
  ([`5f19da9`](https://github.com/bec-project/bec/commit/5f19da921eafab72d0f64f437e4d49fa7afff988))

- Fixed rpc configuration updates to also update the cache
  ([`e4ef9b7`](https://github.com/bec-project/bec/commit/e4ef9b725fc29502b175a20df2adf0c418024db7))

- Fixed status callback for cbs where the device is passed on
  ([`d110711`](https://github.com/bec-project/bec/commit/d1107119da4b40f0ed119ed578593a75b49f1f38))

### Testing

- Added e2e test for read_configuration and limit updates
  ([`7cdf4c3`](https://github.com/bec-project/bec/commit/7cdf4c3d47f1f8da5c0f8dccf0dd987772ed0154))

- Fixed typo
  ([`4fae99c`](https://github.com/bec-project/bec/commit/4fae99c0db47bd9971ea220d2f23d5dd7176db6f))

- Fixed typo
  ([`2405053`](https://github.com/bec-project/bec/commit/2405053ff08b1bb530ae0f64cd9f07a128b199ed))


## v0.59.1 (2024-01-29)

### Bug Fixes

- Fixed bug in device limit update
  ([`c347a84`](https://github.com/bec-project/bec/commit/c347a84ad17c23f20866d90c019b7c705e52110a))

### Refactoring

- Add sotwaretrigger to repr and show_all
  ([`d0d8db0`](https://github.com/bec-project/bec/commit/d0d8db0c46f403fd0060fdd45e8d3054a7c006f7))

- Fixed formatting for black24
  ([`6d05dc2`](https://github.com/bec-project/bec/commit/6d05dc2f7f920721d6f7421153d31bff20d0004b))


## v0.59.0 (2024-01-25)

### Bug Fixes

- Fix configupdate for readOnly
  ([`371175a`](https://github.com/bec-project/bec/commit/371175a2959d3c668a660f7cce87fa27fbc12769))

### Documentation

- Complement documentation
  ([`356374e`](https://github.com/bec-project/bec/commit/356374e59ccc2653e979fefd0bc8b571717ed126))

### Features

- Add softwareTrigger to dev._config
  ([`675e74b`](https://github.com/bec-project/bec/commit/675e74b42e2038d219e59e5b24b5a94ae6d4ca54))

### Refactoring

- Add softwareTrigger to avail keys in scibec/config_helper
  ([`5bc85d2`](https://github.com/bec-project/bec/commit/5bc85d282fc50ce82d681ba334c234b0666991e8))

- Remove DEBUG level scan_server
  ([`8246365`](https://github.com/bec-project/bec/commit/82463655aa910afc5347fabcd7652d79c0d0764a))

- Renamed detectors to get_software_triggered_devices and fixed access; closes #172, #173
  ([`98c7136`](https://github.com/bec-project/bec/commit/98c7136a3af29e03bce526189796f3fdf32820f5))

### Testing

- Add test for config_handler; available keys for update
  ([`eed15d6`](https://github.com/bec-project/bec/commit/eed15d6e8a864cf8327027e5c12b050a25e5e42c))

- Add test for get_software_triggered_devices
  ([`70ec095`](https://github.com/bec-project/bec/commit/70ec095e36a9a37dccd94ae63eb46fc6c763083c))

- Complement config_update_test with onFailure key
  ([`b69dccb`](https://github.com/bec-project/bec/commit/b69dccbf9693116a0e61c8633ca095a2dc0f263c))


## v0.58.1 (2024-01-25)

### Bug Fixes

- Minor client improvements
  ([`b58aa12`](https://github.com/bec-project/bec/commit/b58aa12c16dc2a9c8adde4685ce5e90ccc95cfc7))


## v0.58.0 (2024-01-24)

### Bug Fixes

- Fixed cm exit
  ([`09d231a`](https://github.com/bec-project/bec/commit/09d231a783036b71e8a9d9edfaa7408ae8b69fd7))

- Fixed context manager
  ([`52a2cdc`](https://github.com/bec-project/bec/commit/52a2cdc8d1596e988d4a7f18acef4a179fc820ad))

- Fixed scan_to_csv export and scan_export cm
  ([`07654ec`](https://github.com/bec-project/bec/commit/07654ec0432201d44a4311d09efa62084f65c49e))

### Documentation

- Updated the docs
  ([`979e1d6`](https://github.com/bec-project/bec/commit/979e1d6b84beb6223384a2996b1fb2acdfa00d41))

### Features

- Added context manager for scan export
  ([`00e4fbf`](https://github.com/bec-project/bec/commit/00e4fbfe4567bb75151f310703ac22f4bb2eb483))

### Refactoring

- Remove unnecessary imports after merge resolve
  ([`2d46e5d`](https://github.com/bec-project/bec/commit/2d46e5d8bd322c5a179aeca01de72f7f62dc6bfa))

### Testing

- Add and fixed tests for scan_to_csv and scan_export_cm
  ([`260ff38`](https://github.com/bec-project/bec/commit/260ff38d6223c4b34ba78d54dae10ce9e904974c))

- Add end-2-end test for scan_export_cm; closes #81,#161
  ([`425a658`](https://github.com/bec-project/bec/commit/425a658e6ab074b569ecf7cb885662d85939c063))

- Added tests for metadata handler; closes #174
  ([`624613c`](https://github.com/bec-project/bec/commit/624613c3bdbde8974f019c3a3314ec1b2b854732))

- Latency bodge
  ([`f6de378`](https://github.com/bec-project/bec/commit/f6de378f5a930249acb906b3fc2277b8a275a1d5))

- Removed time.sleep due to merge resolve
  ([`f2a947b`](https://github.com/bec-project/bec/commit/f2a947b967ce941b8c90d2fd6e7d2061a3cdef01))


## v0.57.2 (2024-01-24)

### Bug Fixes

- Fixed scihub error handling
  ([`a58b23d`](https://github.com/bec-project/bec/commit/a58b23d1007f01ae2b892c49904382d896696d4a))

### Documentation

- Added ophyd-test to documentation
  ([`5809882`](https://github.com/bec-project/bec/commit/58098821e950ed15e70c558cb389766ff3165779))


## v0.57.1 (2024-01-24)

### Bug Fixes

- Remove deviceType from device config and backend; closes #171
  ([`3cb7ae7`](https://github.com/bec-project/bec/commit/3cb7ae7cf97b1772e8c4f614bf67c87eeb36724f))

### Testing

- Remove test_wait_for_trigger temporary due to dependency on deviceType
  ([`a039cd5`](https://github.com/bec-project/bec/commit/a039cd56cd3785a7680ec1102e2f2f33fecbfb07))


## v0.57.0 (2024-01-24)

### Bug Fixes

- Added default schema
  ([`0f8875d`](https://github.com/bec-project/bec/commit/0f8875d1faaf2ee5629d5096bb5e1660dd045f80))

### Features

- Made some methods staticmethods to simplify their access
  ([`bbddd50`](https://github.com/bec-project/bec/commit/bbddd50f5eb5aa53e8e65b5d2b139dc74fa24ed3))

### Refactoring

- Moved scibec validator to bec_lib
  ([`d338efe`](https://github.com/bec-project/bec/commit/d338efeb52cb8bbd304e33c3c38cfde4b9a48338))


## v0.56.3 (2024-01-23)

### Bug Fixes

- Add 'add' to message again
  ([`d99230a`](https://github.com/bec-project/bec/commit/d99230a45fefa412c5b2678c554575b1b27afc13))

- Bugfix for deviceConfigMessage validation
  ([`1c2a7d1`](https://github.com/bec-project/bec/commit/1c2a7d1850911ee9a8c45f04dea0622d056785a7))

- Disabled config updates on scibec
  ([`78b5cd6`](https://github.com/bec-project/bec/commit/78b5cd66d1391fd855d1913b1a1ea655c86787a6))

### Testing

- Added e2e test for config updates
  ([`01a5c78`](https://github.com/bec-project/bec/commit/01a5c7801cb0e6ce0505be2b999f1791c6b7b21e))

- Added test for DeviceConfigMessage actions
  ([`ce1adf4`](https://github.com/bec-project/bec/commit/ce1adf48000674392ec178a82f909193c184e271))


## v0.56.2 (2024-01-23)

### Bug Fixes

- Fixed client shutdown; closes #168
  ([`869215b`](https://github.com/bec-project/bec/commit/869215bdc28bf8f4a90ea9ca0a9c017d26fe7d9b))


## v0.56.1 (2024-01-23)

### Bug Fixes

- **service**: Use thread termination event to wait instead of time.sleep
  ([`fd39c7c`](https://github.com/bec-project/bec/commit/fd39c7c667d3f06cc411501feaf6a9e614071516))


## v0.56.0 (2024-01-19)

### Bug Fixes

- Fixed scibec readonly token update
  ([`b6ce07e`](https://github.com/bec-project/bec/commit/b6ce07e65bbcacb36f1cec716db03ffaaff2a009))

### Build System

- Added py-scibec dependency
  ([`eeedcc1`](https://github.com/bec-project/bec/commit/eeedcc1b53ae8d3f92e47dc79ec4f22a7a713a02))

### Continuous Integration

- Trigger pipelines should only run merge requests
  ([`fbf013d`](https://github.com/bec-project/bec/commit/fbf013ddcf386f4db0be211f24c7f1b87818a386))

### Features

- Added file content and credential messages
  ([`416dd7e`](https://github.com/bec-project/bec/commit/416dd7e1138ce37955fdb7fbfdfd1854771afc2a))

- Added filecontent message
  ([`cade103`](https://github.com/bec-project/bec/commit/cade10350cb4b83ee4819f81c178548cf5694a4b))

- Added scibec and file content endpoint
  ([`b366414`](https://github.com/bec-project/bec/commit/b366414e5ca66b37c309fae7cc33651e989db0fe))

- Upgraded to new scibec structure
  ([`b72894a`](https://github.com/bec-project/bec/commit/b72894a5f6d2d013fe66df28ca7de29e89125890))

### Refactoring

- Cleanup and tests for new scibec connector
  ([`daea3a3`](https://github.com/bec-project/bec/commit/daea3a31cea04c8647ec5da1432272333ad1b409))

- Moved repeated timer to separate file
  ([`5dd9e28`](https://github.com/bec-project/bec/commit/5dd9e28a23f861fb8fabe69508307630d52ad50d))

- Removed scibec and mongodb config settings
  ([`8c9ff08`](https://github.com/bec-project/bec/commit/8c9ff08310ba593c3c09d225b7c4fd350dd44c4c))

### Testing

- Fixed test data
  ([`2af044b`](https://github.com/bec-project/bec/commit/2af044b2c6ed7b16e046c7a001151dc8b6625a7e))


## v0.55.0 (2024-01-19)

### Bug Fixes

- Add valid check for actions in DeviceConfigMessage
  ([`3a52b19`](https://github.com/bec-project/bec/commit/3a52b1914534c373057e419eb6cec247575929a5))

### Continuous Integration

- Made bec-widgets job optional for now
  ([`e662e01`](https://github.com/bec-project/bec/commit/e662e01b2077d835915088847cd354ddc0d8fc6f))

### Documentation

- Reviewed docstring of BECMessages
  ([`a4be91f`](https://github.com/bec-project/bec/commit/a4be91f37c43de4d19101578a94a62d629309427))

- Updated scanqueuemessage doc string
  ([`fd89d86`](https://github.com/bec-project/bec/commit/fd89d8648f84c8029e141b646b99b2c9c13a68e1))

### Features

- Add monitor endpoint, device_monitor, and DeviceMonitor message
  ([`0a292b0`](https://github.com/bec-project/bec/commit/0a292b0363479c288be029be35b8560b79a69d29))

- Add sub for monitor, and callback; closes #158
  ([`4767272`](https://github.com/bec-project/bec/commit/4767272778693d8abd2db81aecf77ebd5d5f3109))

### Refactoring

- Change datatype from value to np.ndarray
  ([`96abc04`](https://github.com/bec-project/bec/commit/96abc040bce09a44174cc97aa535f879694e84dd))

- Refactor devicemonitormessage, remove datatype
  ([`e612ce9`](https://github.com/bec-project/bec/commit/e612ce9746360222bcf2d9f36bfebee49dccce4b))

### Testing

- Add test for monitor cb
  ([`f802e60`](https://github.com/bec-project/bec/commit/f802e60b5c3e6025610b3adf8c206f673452c4bc))


## v0.54.0 (2024-01-18)

### Continuous Integration

- Added ophyd_devices trigger job
  ([`6e22b5c`](https://github.com/bec-project/bec/commit/6e22b5c525380f6ee37e23c8df20e9d16969b09a))

- Added trigger job for bec-widgets
  ([`acf18ce`](https://github.com/bec-project/bec/commit/acf18ceeeb5541a5fb373a6aa17d8b78f395949c))

### Features

- **config**: Allow both .yaml and .yml files as valid config files
  ([`a1ca26d`](https://github.com/bec-project/bec/commit/a1ca26dbd21823ae21eef359b8592a8c8749d300))


## v0.53.0 (2024-01-12)

### Bug Fixes

- Bec_plotter.py fixed redis source format for new config style
  ([`6ce1e3a`](https://github.com/bec-project/bec/commit/6ce1e3a4845db248a89617573fb30350554364da))

- Bec_plotter.py live monitoring fixed to new config structure of BECMonitor
  ([`6dca909`](https://github.com/bec-project/bec/commit/6dca90902c7ac8938c64ee2a2f00d2bd54c00c0b))

### Documentation

- Becplotter docs updated in GUI section
  ([`6bf51ab`](https://github.com/bec-project/bec/commit/6bf51abeeac9684c1ef78077f2f8abcf7133ee06))

### Features

- Gui config dialog for BECMonitor can be opened from bec IPYTHON client
  ([`bceb55d`](https://github.com/bec-project/bec/commit/bceb55d18880f773cb9cabd265e53793b211a3f5))

### Refactoring

- Bec_plotter.py changed attribute names for setting new configs
  ([`46682e6`](https://github.com/bec-project/bec/commit/46682e626aaa5dbf21911f8a3336e7f86f6ddce7))

- Clean up print statements
  ([`ba7e08c`](https://github.com/bec-project/bec/commit/ba7e08cd38b42823709d85589878cb0fa40a6308))

### Testing

- Test_bec_plotter.py all tests fixed
  ([`d6d888f`](https://github.com/bec-project/bec/commit/d6d888f197e4f61bf825a8bc4ed8db47ca3d61bb))

- Test_bec_plotter.py setting 'scan_segment' sources and labels fixed, redis tests disabled
  ([`e3c2509`](https://github.com/bec-project/bec/commit/e3c2509e103971f7e0b446bddcdb7a9ad2d99296))


## v0.52.9 (2023-12-22)

### Bug Fixes

- Read commented in DeviceBase
  ([`2365dff`](https://github.com/bec-project/bec/commit/2365dff4a6a02d27e6cb1e28d3fd2b9dc7cb78b7))

- Wrong reference for 'monitor' - changed from DeviceBase to Device
  ([`17cc883`](https://github.com/bec-project/bec/commit/17cc883355c21299a062fd5bf1490d0f033f0414))

### Continuous Integration

- Fix cobertura for gitlab/16
  ([`7bffd0e`](https://github.com/bec-project/bec/commit/7bffd0e4f84b4f7b629de1a52c3b6d75c75761c4))

Fix #156

- Revert to ophyd master
  ([`e06ef69`](https://github.com/bec-project/bec/commit/e06ef69b779854216db53e7a31d8a3734040cf05))

### Testing

- Added tests for describe and describe_configuration
  ([`f08b7d4`](https://github.com/bec-project/bec/commit/f08b7d414a7244d7b56d2eb6f5b01d85c0df98f9))


## v0.52.8 (2023-12-18)

### Bug Fixes

- Fixed scan def cleanup
  ([`4be4252`](https://github.com/bec-project/bec/commit/4be425277b561b9228982bc55d7f3980cf2bf98f))


## v0.52.7 (2023-12-18)

### Bug Fixes

- Fixed import of device manager
  ([`f162633`](https://github.com/bec-project/bec/commit/f1626336b271b8a231ca46e175ae845ba4071eb6))

- Service should wait for device info
  ([`67b292f`](https://github.com/bec-project/bec/commit/67b292fa0d5ab67cf945db6d12a1f92db642d3a3))

- Wait for scihub server to become ready
  ([`77232ac`](https://github.com/bec-project/bec/commit/77232ac75f12acc9a754a2b5dcd76fa922340b7b))

### Continuous Integration

- Added logs for scihub and dap
  ([`51ff5df`](https://github.com/bec-project/bec/commit/51ff5dff3a80503c3d0f24e0bcd353752a2eb787))

- Fixed scihub log path
  ([`794556d`](https://github.com/bec-project/bec/commit/794556d265d7ff28578625d3c63de1f8a65dcc0e))

- Preliminary fixed ophyd devices branch
  ([`e24f046`](https://github.com/bec-project/bec/commit/e24f0464b56c258c83b9dbd928132e4875a5fa4c))

### Refactoring

- Changes related to devicemanager refactoring
  ([`c01e6df`](https://github.com/bec-project/bec/commit/c01e6df2c1355e0387141aed4f90af58bc9be2a0))

- Deprecated devicemanager_client
  ([`9acba36`](https://github.com/bec-project/bec/commit/9acba36aa20057454b10336d0ce97991d121875e))

### Testing

- Fixed test after device_manager refactoring
  ([`14b2c9d`](https://github.com/bec-project/bec/commit/14b2c9dbdf68e761ba1678d7b4b939ef5ba8f1fb))

- Fixed tests - service are now waiting for device server
  ([`b70421c`](https://github.com/bec-project/bec/commit/b70421c30965fa9eb4969d9b070167ba9700977d))


## v0.52.6 (2023-12-18)

### Bug Fixes

- Fixed limit update for epics pvs; closes #113
  ([`fce2520`](https://github.com/bec-project/bec/commit/fce2520e38c80b1d2c01349b5f0d02d8eaf2a3bd))


## v0.52.5 (2023-12-18)

### Bug Fixes

- Fixed scan data namespace clash; closes #141
  ([`8c4cee8`](https://github.com/bec-project/bec/commit/8c4cee824bd0a2d623fd65f63f7a91347c79076d))


## v0.52.4 (2023-12-17)

### Bug Fixes

- Fixed config update
  ([`377e820`](https://github.com/bec-project/bec/commit/377e82085c704fd2052f2bc3ad01fd1fe686a1c7))

- Fixed config update
  ([`76d1e06`](https://github.com/bec-project/bec/commit/76d1e063794cadcf2dbfeefaa3fc0de9b04a7019))

### Testing

- Test to ensure rid is forwarded
  ([`b24c5eb`](https://github.com/bec-project/bec/commit/b24c5eb50e164d2cc2ee5bed6a0b0f5486e446a6))


## v0.52.3 (2023-12-16)

### Bug Fixes

- Fixed bug in alarambase that would prohibit error propagation
  ([`4c88bc6`](https://github.com/bec-project/bec/commit/4c88bc687d90acbd41d0312cda42d1c049dd9423))

- Fixed log level init
  ([`5280dad`](https://github.com/bec-project/bec/commit/5280dadc0640eaf1d3fbdf2fd6b1ce37a9f8f8ff))

- Fixed timeout error in config_helper
  ([`6e75ca7`](https://github.com/bec-project/bec/commit/6e75ca73bdd11740bcb5fd71c8157d8d66f05b53))

- Removed bec logger overwrite that prohibited log outputs
  ([`acbcb69`](https://github.com/bec-project/bec/commit/acbcb69eb22900ca6679d6b059189761a34f4ece))

### Continuous Integration

- Removed test utils from coverage report
  ([`2f110d3`](https://github.com/bec-project/bec/commit/2f110d34994540fcd9510dcc653f64f6e2bdfc86))

### Refactoring

- Added console log
  ([`d0c898b`](https://github.com/bec-project/bec/commit/d0c898b53efa62bda2e97bf7dd2f1b985fed1151))

- Added dedicated console log file
  ([`05a59d3`](https://github.com/bec-project/bec/commit/05a59d37f2e253189eeba84e9e5cccbfa60834b1))

- Changed log level for clearing the alarm stack from warning to info
  ([`e9e341d`](https://github.com/bec-project/bec/commit/e9e341dfed5af5f1ae1e59f1c0700e447838e966))

### Testing

- Added wait_for_service_response tests
  ([`339d95f`](https://github.com/bec-project/bec/commit/339d95fd3552404bb0f22dbd730068bba39f2020))


## v0.52.2 (2023-12-15)

### Bug Fixes

- Fixed wm behaviour
  ([`4ea93dc`](https://github.com/bec-project/bec/commit/4ea93dcd48a893e95e65a69c91b485d96c49df12))


## v0.52.1 (2023-12-15)

### Bug Fixes

- Added service acknowledgement for config updates; closes #79
  ([`bc1c43e`](https://github.com/bec-project/bec/commit/bc1c43e2da1775b191dd168ab96599a4ada425cc))

- Fixed config_ack for incomplete messages
  ([`16c0a1d`](https://github.com/bec-project/bec/commit/16c0a1d4d4d023239afb12862a5732ec5187cf6f))

### Refactoring

- Added bec service to builtins
  ([`1d4ae20`](https://github.com/bec-project/bec/commit/1d4ae20b1a6d846db62806c0669f285a9724c760))

### Testing

- Fixed device server tests for config update
  ([`1fbf50c`](https://github.com/bec-project/bec/commit/1fbf50cf42e2c147f117bb0bcfbcdab82198ccb7))

- Fixed scan guard tests for service response
  ([`b8300c0`](https://github.com/bec-project/bec/commit/b8300c06daab3b7dac74bcb4ece34e8fdee78e5c))

- Fixed tests for config ack
  ([`2e7a09b`](https://github.com/bec-project/bec/commit/2e7a09b1ce7ce17d4eb4854563477edb524c6a73))


## v0.52.0 (2023-12-15)

### Features

- Added channel monitor as cli script
  ([`31cc15f`](https://github.com/bec-project/bec/commit/31cc15f204ded7d368ef384cdb04448c18c5bc3f))

### Refactoring

- Cleanup and tests
  ([`ab89952`](https://github.com/bec-project/bec/commit/ab899524b4270872c57b8f937708781b22ade4c4))

- Update configs; relates to 2db65a385524b81bef1943a2a91693f327de4213
  ([`8dbf4c7`](https://github.com/bec-project/bec/commit/8dbf4c79d4f83e661a7e55839608194c6b681b65))


## v0.51.0 (2023-12-14)

### Bug Fixes

- Added option to read configuration from redis
  ([`f7acd4c`](https://github.com/bec-project/bec/commit/f7acd4cb0646a660676fd5561d23c00bc7157fcc))

- Added read_config on init
  ([`3529108`](https://github.com/bec-project/bec/commit/352910812487104a87b8d95dc37d1c2164574074))

- Fixed bug in config readout
  ([`4a640e5`](https://github.com/bec-project/bec/commit/4a640e5ea05e7842fbfda851d79b97af0a801720))

- Fixed ctrl c for rpc calls for unresponsive backends
  ([`6341059`](https://github.com/bec-project/bec/commit/6341059dd068daf5fd6cce4947901cc3c3dcbe31))

- Fixed read and read_config for cached readouts
  ([`ba2a797`](https://github.com/bec-project/bec/commit/ba2a797dfbd67b98b931b5f299bf53a2f4ed71a2))

- Fixed readout for omitted signals
  ([`532d142`](https://github.com/bec-project/bec/commit/532d142860ddac19dc1581db895514608dcfb65f))

- Fixed rpc calls for read_configuration
  ([`3a475e7`](https://github.com/bec-project/bec/commit/3a475e7e6e0cf46420bfe99a7b9747110d608412))

### Build System

- Fix python requirement
  ([`4bfe93f`](https://github.com/bec-project/bec/commit/4bfe93f6e2a5dc7b4fc778b043e5ccb5ba234674))

- Fixed install script to update the conda deps if they are outdated
  ([`abedd5e`](https://github.com/bec-project/bec/commit/abedd5e95489c20519e48215f58ed5d7a9eca824))

### Documentation

- Updated docs for cached config readouts
  ([`c33a66e`](https://github.com/bec-project/bec/commit/c33a66ef00e096eb94f01b9205c2594fa5c81673))

### Features

- Added message endpoint for read_configuration
  ([`3faf40a`](https://github.com/bec-project/bec/commit/3faf40a218716bfa1c4271b01d9f516f93e03807))

### Refactoring

- Cleanup
  ([`d142862`](https://github.com/bec-project/bec/commit/d14286295ae0097b81c25fd9ac755e39186fe53d))

### Testing

- Added tests for read_configuration
  ([`fed2c9f`](https://github.com/bec-project/bec/commit/fed2c9f35a63f5da371c6815914960935afd0239))

- Fixed dm client tests for rpc interface
  ([`b0903ba`](https://github.com/bec-project/bec/commit/b0903ba15ad673a322fd6330cb0bf3f15811f952))

- Fixed test for omitted signals
  ([`cd07fd2`](https://github.com/bec-project/bec/commit/cd07fd2385bbfcc54a543648abbc87a2ffff8a87))


## v0.50.2 (2023-12-11)

### Bug Fixes

- Fix devicemanger get_deviceType_devices bug and add test
  ([`4aa9ba4`](https://github.com/bec-project/bec/commit/4aa9ba4a8ef26fef2ad51ef72cd600ce624b7542))

- Remove redundant imports
  ([`4a27b9a`](https://github.com/bec-project/bec/commit/4a27b9a1ecde763e913774f8c23b308f79e7a181))

### Build System

- Pin typeguard/4.0.1
  ([`1c44912`](https://github.com/bec-project/bec/commit/1c44912a9027474a954b37cd89119879df3fcf66))

All typeguard/3.x versions, and 4.0.0 have an issue with class property decorator

### Continuous Integration

- Default to python39
  ([`ad10fb9`](https://github.com/bec-project/bec/commit/ad10fb93f512c5bf36179caa38d5b3605943a64a))

- Include python312
  ([`36e33cb`](https://github.com/bec-project/bec/commit/36e33cbf807ba1a9b8b3d4ba09af5a565221d30b))

### Documentation

- Add docs for read and get interface access; closes #125
  ([`fad8662`](https://github.com/bec-project/bec/commit/fad86626a4ff3d2b11a1c3dc2cf34baeb0bb5777))

- Add fields to developer.ophyd as fillers
  ([`3c64df3`](https://github.com/bec-project/bec/commit/3c64df327af16485d68dc2d8d2b1a312af67f932))

- Address merge comments
  ([`c288a4e`](https://github.com/bec-project/bec/commit/c288a4ee5341502abc08fd2d163462e1c8d95cbd))

- Fix docs, merge ophyd_devices into ophyd in developer documentation
  ([`26dabe6`](https://github.com/bec-project/bec/commit/26dabe6f31333e8417ad569b484c80e1e4026f23))

- Fix typos, add links to requirements
  ([`ab7a9fa`](https://github.com/bec-project/bec/commit/ab7a9faf747cc8b4954050186113bdb2ab1ee4a7))

- Update docs, change software limits for motor
  ([`e2a41c8`](https://github.com/bec-project/bec/commit/e2a41c8b12e74debfe68a3d44424bde5a5841984))

- Update user docs, read and get; closes #125, #150
  ([`6cf5cfa`](https://github.com/bec-project/bec/commit/6cf5cfa7a5456fb899fd649b89b2c22293d2a3d8))

### Refactoring

- Adapt python310 Union and Optional style
  ([`a68a809`](https://github.com/bec-project/bec/commit/a68a809c4b832db9c59a8cef65f9f7f8c22ebac8))

- Replace deprecated imports from typing
  ([`ac14a73`](https://github.com/bec-project/bec/commit/ac14a73e8b088b09070f18daa431cb239d0cd2e5))


## v0.50.1 (2023-12-11)

### Bug Fixes

- Fixed decorator order and raised error for new typeguard version
  ([`8b610c2`](https://github.com/bec-project/bec/commit/8b610c2ee88229122991892490b053fae3454b20))

### Build System

- Support "typeguard>=3"
  ([`1ac5e5e`](https://github.com/bec-project/bec/commit/1ac5e5e8d39f169c514a4189a9bad829c8c641f5))

### Refactoring

- Removed scibec
  ([`ac0b93d`](https://github.com/bec-project/bec/commit/ac0b93d4f115ca0cede09184e09f12838f967efd))


## v0.50.0 (2023-12-11)

### Bug Fixes

- Added implicit ophyd device name assignment
  ([`6b497e2`](https://github.com/bec-project/bec/commit/6b497e2536a993a4ba870a146a5fc824408907bc))

- Allow empty signals
  ([`cdd1d0c`](https://github.com/bec-project/bec/commit/cdd1d0cba0816692faeec9bd74ea97b4043579d3))

- Clean up device_manager and scan_worker, add tests for baseline_devices; closes #144, #98
  ([`7d5c03b`](https://github.com/bec-project/bec/commit/7d5c03b7b9a8683d59773fc0b7e5f0830e563519))

- Fix baseline_update
  ([`c39bdc1`](https://github.com/bec-project/bec/commit/c39bdc13b536e49909584c2398dd6ec595e67d27))

- Fixed bec_lib after refactoring
  ([`9317220`](https://github.com/bec-project/bec/commit/93172203b6292dfe8399fb47a277263002f94f01))

- Fixed bug and tests
  ([`beb0651`](https://github.com/bec-project/bec/commit/beb065124d0fcac7df4469a76a552ff057bd6a52))

- Fixed config update in config handler
  ([`cdbaf0c`](https://github.com/bec-project/bec/commit/cdbaf0c6c4132af326ece5feb35ba302efa84c72))

- Fixed config update in devicemanager
  ([`46d1cf9`](https://github.com/bec-project/bec/commit/46d1cf97dffbc14f97eddfdc6dac0161e5861216))

- Fixed demo config
  ([`ab399cc`](https://github.com/bec-project/bec/commit/ab399cc934f186d286595bfd325fe7d78f31351e))

- Fixed devicemanager for missing deviceConfig
  ([`daa0e8e`](https://github.com/bec-project/bec/commit/daa0e8e5e24518839b68dfebaca74f579ca49a9f))

- Fixed fly scan sim
  ([`50fc302`](https://github.com/bec-project/bec/commit/50fc30216bd44ec46118fba5e37def56b859c8a5))

- Fixed scan server after config refactoring
  ([`9397918`](https://github.com/bec-project/bec/commit/939791889f9403c597ce7cbcb5f5c401ae6747a1))

- Fixed update for deviceConfig
  ([`1b81ffb`](https://github.com/bec-project/bec/commit/1b81ffb3323ed560f1791587f612b8dfb254f6c4))

### Documentation

- Update documentation to new config structure
  ([`f38ddc3`](https://github.com/bec-project/bec/commit/f38ddc3854611d8bf63a776749d418af870511d3))

### Features

- Relaxed rules on deviceConfig schema; removed need for adding name
  ([`26d3f45`](https://github.com/bec-project/bec/commit/26d3f45c7838c0cc60b649b3051ee5ce4e758ad5))

- Removed acquisition group and status from device config
  ([`5f48362`](https://github.com/bec-project/bec/commit/5f4836266761f880e98e0798d0046d477a4b1e43))

### Refactoring

- Removed name and labels from config
  ([`4e83b65`](https://github.com/bec-project/bec/commit/4e83b65c995ae51b286cf020d5868fc8d500db17))

- Removed old e2e config
  ([`a84b07d`](https://github.com/bec-project/bec/commit/a84b07dea7a1163fba69271400dbab44be3a7c69))

### Testing

- Fixed fly sim test
  ([`ddfe126`](https://github.com/bec-project/bec/commit/ddfe126e8786a2d9c8d08da2ee5b907c7fa5241b))

- Fixed tests after config refactoring
  ([`c9d703f`](https://github.com/bec-project/bec/commit/c9d703f78e5426038370ae20a062d4137702c988))


## v0.49.2 (2023-12-11)

### Bug Fixes

- Added wheel for bec server install
  ([`7f51416`](https://github.com/bec-project/bec/commit/7f514168c027031d8dacd4b7ec539c78a468b543))

### Continuous Integration

- Added issue templates
  ([`a207011`](https://github.com/bec-project/bec/commit/a2070113c7497c5f16cbd618c34b2fc91f6e4232))

- Added merge request template
  ([`d690f06`](https://github.com/bec-project/bec/commit/d690f0670a72d11875aa3c233bf634512314bc83))

- Updated default mr template
  ([`350545c`](https://github.com/bec-project/bec/commit/350545ce67eb13e1ada9c1839ce8814d6c76c776))

- Updated heading in default mr template
  ([`abbb761`](https://github.com/bec-project/bec/commit/abbb761838ab893516133e5a0ebbb45875e7e613))

### Documentation

- Updated install information for bec dev
  ([`6ede847`](https://github.com/bec-project/bec/commit/6ede847b3e02593241420c37425659429729f823))


## v0.49.1 (2023-12-08)

### Bug Fixes

- Fixed .get inconsistencies
  ([`83af812`](https://github.com/bec-project/bec/commit/83af8127da11c80a47e05e375080c89bcc76716e))


## v0.49.0 (2023-12-07)

### Bug Fixes

- Added missing set and append functions
  ([`716f80e`](https://github.com/bec-project/bec/commit/716f80e2ca6d6383f8dc630680e54984d3375da6))

- Fixed print_log; added tests
  ([`9028693`](https://github.com/bec-project/bec/commit/9028693a3cd8ebc81ac6dc4832edc52732cd6444))

- Fixed show for manually closed figures
  ([`b68f38e`](https://github.com/bec-project/bec/commit/b68f38e866a1a4806e7cc79c840cabfebbd27d38))

- Removed hard-coded link to widgets
  ([`3a99554`](https://github.com/bec-project/bec/commit/3a99554b7e5310606a968c5e71eb7942d1381aaa))

### Features

- Added first version of bec_plotter
  ([`6c485c7`](https://github.com/bec-project/bec/commit/6c485c7fcdcd2cbea3b5486c5df531c215e4fa13))

- Added gui endpoints and messages
  ([`6472e4e`](https://github.com/bec-project/bec/commit/6472e4ef94b8100405e1c2e0011fd0a8c698a300))

### Refactoring

- Cleanup
  ([`1561631`](https://github.com/bec-project/bec/commit/1561631eb932488cf1d9c4dd146f9fbdf0c8a4db))

- Minor refactoring; added test for print_log
  ([`6dd3dfe`](https://github.com/bec-project/bec/commit/6dd3dfea4e9781f23466b244472ea816056b5d41))

### Testing

- Added tests for bec plotter
  ([`0a70743`](https://github.com/bec-project/bec/commit/0a707436c7a2631613cbafaaa22a2f37b8d253bd))


## v0.48.0 (2023-12-05)

### Bug Fixes

- Fixed bug in readout for hinted and normal signals
  ([`bcd2433`](https://github.com/bec-project/bec/commit/bcd243361af8eccd0771bc6950fcc3f86689c664))

- Fixed cached readout for .get; closes #137
  ([`4fc35ca`](https://github.com/bec-project/bec/commit/4fc35cadc161c1b39fc5a891ab7150f9b043b9f0))

- Made rpc interface more consistent with ophyd
  ([`e0e3a71`](https://github.com/bec-project/bec/commit/e0e3a7158cee84c56f4ce82657e36ff88b18a36b))

### Documentation

- Cleanup developer docs; remove usage folder
  ([`329e30b`](https://github.com/bec-project/bec/commit/329e30b722843d66396e3e4bd8fc0d12660a6f06))

- Fixed paragraph level
  ([`01bba51`](https://github.com/bec-project/bec/commit/01bba51da191e07d3ada050a78e031248cb4dd50))

- Improved introduction
  ([`1c82e80`](https://github.com/bec-project/bec/commit/1c82e80960d27e85b73e1f308f06f44ec5a54316))

- Rem typo and add link in data_access
  ([`b5c7453`](https://github.com/bec-project/bec/commit/b5c7453445c565e99703436b88420d7b8f98d197))

- Rem typos in cli section
  ([`ede65af`](https://github.com/bec-project/bec/commit/ede65af1c3c189bca137567096537709332c2b18))

- Resolved threadl small typo in install
  ([`3236d1e`](https://github.com/bec-project/bec/commit/3236d1ea725ce78b13828cdf6588e5c836983ef7))

- Review contributing section
  ([`f4ffff3`](https://github.com/bec-project/bec/commit/f4ffff3414baa62446e2810cbd389111d8d53183))

- Review developer page
  ([`c35e0be`](https://github.com/bec-project/bec/commit/c35e0be40d13724d684109dcaea827a14d2a6dae))

- Review install_developer_env
  ([`5e3c10a`](https://github.com/bec-project/bec/commit/5e3c10aca6517e5e4aa5eb00fc585e07f091b48c))

- Reviewd architecture section
  ([`1ac315d`](https://github.com/bec-project/bec/commit/1ac315dc61094eee3d84f8cc2b3cf5b6331b04ba))

- Split ophyd and ophyd_devices
  ([`173eb26`](https://github.com/bec-project/bec/commit/173eb26b0c7469413075bb3766edd4f0ae626866))

### Features

- Added support for namedtuple serialization for rpc
  ([`fd00974`](https://github.com/bec-project/bec/commit/fd00974b05112a7c85eea412a1be89fee3b74822))

### Refactoring

- Remove :sub :val :stream remnants
  ([`ade6ae4`](https://github.com/bec-project/bec/commit/ade6ae4585098b2c5f4cdcc96e3fbbc11ff8c5d6))

- Removed device_server_config_response endpoint; closes #142
  ([`6a0a1be`](https://github.com/bec-project/bec/commit/6a0a1bea803cc94cc80904e1116f8da182a6b2c0))


## v0.47.0 (2023-11-28)

### Documentation

- Fixed link; minor changes
  ([`6acbb66`](https://github.com/bec-project/bec/commit/6acbb66ed7472fb369623e2ad55cc2c1835886ed))

- Fixed typos and links in user section
  ([`dc0d611`](https://github.com/bec-project/bec/commit/dc0d611ddc88f5c35922fd5366e20e51c34e1053))

- Refactoring of user section
  ([`487582d`](https://github.com/bec-project/bec/commit/487582d0a124b01c40c8b324f169e92f3d74d978))

### Features

- Added support for starting the bec client with a config
  ([`0379031`](https://github.com/bec-project/bec/commit/0379031fa7653e3cb647ef35cab95426bf5b1130))


## v0.46.1 (2023-11-28)

### Bug Fixes

- Fixed ctrl-c behaviour for report.wait; closes #138
  ([`728b43c`](https://github.com/bec-project/bec/commit/728b43c3f98c26dd337bdfff8bb4afc2fd684b80))


## v0.46.0 (2023-11-28)

### Features

- Added version flag to bec cli
  ([`438e625`](https://github.com/bec-project/bec/commit/438e6258dfd9806227d9ae89f2ae892c557e386a))


## v0.45.4 (2023-11-28)

### Bug Fixes

- Fixed device read for nested devices; closes #134
  ([`eda60c5`](https://github.com/bec-project/bec/commit/eda60c529afea248104279b3152ef9cfcb44b228))


## v0.45.3 (2023-11-28)

### Bug Fixes

- Added missing file
  ([`e82604c`](https://github.com/bec-project/bec/commit/e82604cab5c48e228dbdd0016725c0d3ddc3c659))

- Fixed import in spec_hli
  ([`d5bc55a`](https://github.com/bec-project/bec/commit/d5bc55aa8b047fafb59900394292e62d1a5c1b05))

### Refactoring

- Moved scan report to separate file
  ([`045526a`](https://github.com/bec-project/bec/commit/045526a9ad21ae7756f9e62607a6f1d086c2db04))


## v0.45.2 (2023-11-27)

### Bug Fixes

- Fixed stop instruction for halt
  ([`6eb1081`](https://github.com/bec-project/bec/commit/6eb10810d6de19bbeb9170fd78259864c3ca682c))


## v0.45.1 (2023-11-27)

### Bug Fixes

- Add short delay in case of connection error
  ([`95106d6`](https://github.com/bec-project/bec/commit/95106d6136d2d0a6fb476a422d970dcf830519de))

### Documentation

- Add gauss_scatter_plot
  ([`bf138ca`](https://github.com/bec-project/bec/commit/bf138ca380261e2dcaf6659fceb9ab8f6daa4129))

- Fix style
  ([`cb531c2`](https://github.com/bec-project/bec/commit/cb531c2de16ea41a31c3bf80a31b463c5fbae28d))

- Include comments upon merge request
  ([`e3c3607`](https://github.com/bec-project/bec/commit/e3c3607fef0cb1238b9d3a60a61b3576a5660c14))

- Refactor device configuration.md
  ([`12bd969`](https://github.com/bec-project/bec/commit/12bd969d59dafde22cdd1fd1d0b6c15a16629a52))

- Remove typo
  ([`e017900`](https://github.com/bec-project/bec/commit/e01790062b93a132a6581cd023f215b87e30fc8e))

- Reviewed user documentation
  ([`15316ca`](https://github.com/bec-project/bec/commit/15316caedb0ffa9846b199cc795e3bea3e031386))

- Update docstrings for endpoints
  ([`945297d`](https://github.com/bec-project/bec/commit/945297d4ac4be12c204546f5568a89eb4efb148b))

These updates are based on their actual usage in the code

- Update user guide for installation
  ([`aa5a245`](https://github.com/bec-project/bec/commit/aa5a245b46b46255b78b4f5d1a71898f6c2257bf))


## v0.45.0 (2023-11-24)

### Features

- Add load_demo_config method
  ([`20dfc64`](https://github.com/bec-project/bec/commit/20dfc6497266bb0dde52cd71bd4e88ce7f364571))


## v0.44.2 (2023-11-23)

### Bug Fixes

- Fixed config_init path to config file
  ([`e1a2429`](https://github.com/bec-project/bec/commit/e1a2429fac8756832bcc9937262fb72a8aace592))

- Fixed config_init path to config file. again.
  ([`6b714ef`](https://github.com/bec-project/bec/commit/6b714ef375dd2e9599d462b4091194fbec264f94))

- Fixed packaging of demo_config and openapi_schema
  ([`7f8b1b1`](https://github.com/bec-project/bec/commit/7f8b1b1bbe8dee285b71e221161f0c86ad49dd01))

### Documentation

- Added placeholder for developer doc
  ([`f5a9f7d`](https://github.com/bec-project/bec/commit/f5a9f7dfa6ebf9d23b53fa33764f684228690c11))

- Fixed link to conventionalcommits
  ([`6731a55`](https://github.com/bec-project/bec/commit/6731a559422f7760a39fe160f22298022174bff1))

- Fixed page navigation
  ([`033c535`](https://github.com/bec-project/bec/commit/033c53529e4947422968b258d26347c74b983d3d))


## v0.44.1 (2023-11-22)

### Bug Fixes

- Fixed startup script by adding main guard
  ([`f6b5e9e`](https://github.com/bec-project/bec/commit/f6b5e9e3c708162eb9f07c118e0226d5395f7f20))


## v0.44.0 (2023-11-21)

### Features

- Added GUI config endpoint
  ([`67903a4`](https://github.com/bec-project/bec/commit/67903a47bdcace6fcb9043aa6ad2bcb512260e12))


## v0.43.0 (2023-11-21)

### Bug Fixes

- Fixed scan_data len dunder
  ([`b037b91`](https://github.com/bec-project/bec/commit/b037b91c53b1bbc40224f712bc10787e981add39))

### Continuous Integration

- Added missing scihub tests to additional test jobs
  ([`9345b15`](https://github.com/bec-project/bec/commit/9345b15e7a602a71c3d8e9e33cbbb0ab845f3b51))

### Features

- Added scan_data to simplify the access to the scan storage
  ([`6cfff5a`](https://github.com/bec-project/bec/commit/6cfff5a529650094aa602d3669d96a7637bb79a1))

### Refactoring

- Avoid logger <-> messages circular import
  ([`ddc12ba`](https://github.com/bec-project/bec/commit/ddc12ba9bca8beeae3fb8e80bb11f99e217589be))

This is a preventive measure. The logger module needs LogMessage, and messages module needs
  bec_logger, so as one possible fix, delay LogMessage access in logger.

- Avoid using bec message aliases
  ([`331e653`](https://github.com/bec-project/bec/commit/331e653ee60fad81da511728f369dd7bfed5e1ff))

- Remove redundant BMessage from test_scans
  ([`7ed8937`](https://github.com/bec-project/bec/commit/7ed8937dec7a473953ad1dfb1ac68f50dbc8fc79))

- Revert commits that added temp module stubs
  ([`5d254db`](https://github.com/bec-project/bec/commit/5d254db6b78e684747f2350aacde845935313165))

Fix path to test_service_config.yaml This reverts commit 109453c1ccb3ebc8506e57f549549f99b38e4c8f
  This reverts commit b73e9b3baac660bb7af3fc049c27e3bdb294bba9. This reverts commit
  108dc1179cf191c98b7891605d665371b5a2bca2.

### Testing

- Added test for diid>=target_diid
  ([`298faaf`](https://github.com/bec-project/bec/commit/298faaf14064aef3d74f1b7d2b77ef86605bd8e9))

- Fixed tests for new scan data structure
  ([`cbc3870`](https://github.com/bec-project/bec/commit/cbc38708840b70c185a652990583e64a8018b4a5))


## v0.42.10 (2023-11-19)

### Bug Fixes

- Changes related to new read signature
  ([`80ee353`](https://github.com/bec-project/bec/commit/80ee35371291831e5a9a3be3a7d9a09fadf710c2))

- Fixed readback data mixin
  ([`a396f12`](https://github.com/bec-project/bec/commit/a396f12ec434359ba8735ad466d6fbd75a74aca1))

- Fixed rpc func name compilation
  ([`c576669`](https://github.com/bec-project/bec/commit/c57666949582663124f8b7b02f1707f41164f35c))

- Read through rpc updates the redis entries
  ([`52f9a4e`](https://github.com/bec-project/bec/commit/52f9a4eceef70b1fb9df428ff9740eac7a45ea2f))

### Refactoring

- Clean up
  ([`d362aae`](https://github.com/bec-project/bec/commit/d362aae393288f4795724b88fda9cb6f1062a679))

- Improved dev.show_all
  ([`f491d40`](https://github.com/bec-project/bec/commit/f491d40bac12550ecbc5234e9254b749c88f5bf5))

- Made rpc a public method of rpc mixin
  ([`1bb2f0c`](https://github.com/bec-project/bec/commit/1bb2f0c101aa6c8406e20d87d36582bc59d2cb55))

- Moved rpc logic to separate mixin class
  ([`ba3e780`](https://github.com/bec-project/bec/commit/ba3e780400db04ab11d808a8291bc03e6dbeb1c6))

- Moved rpc_mixin to separate file
  ([`35ea586`](https://github.com/bec-project/bec/commit/35ea586f5a8220a7a8421136a598f38469fb2d08))

### Testing

- Added tests for rpc mixin class
  ([`773168e`](https://github.com/bec-project/bec/commit/773168e2da87625fe9ee96824c53e88ce005ecb8))

- Fixed floating point precision for table report
  ([`6e5a827`](https://github.com/bec-project/bec/commit/6e5a8272f8c2d1158ca1c816e0720fce588594a2))

- Fixed show_all test
  ([`de375b7`](https://github.com/bec-project/bec/commit/de375b7579adec4dba5da2ef127bf4fd549eb195))

- Updated rpc test
  ([`16d4d86`](https://github.com/bec-project/bec/commit/16d4d8618ad850de7229c315fe6af0e03aaab9d4))


## v0.42.9 (2023-11-19)

### Bug Fixes

- Clean up __init__
  ([`ab9a5e3`](https://github.com/bec-project/bec/commit/ab9a5e3fa516dbb599400f2cf796169af98ec5e2))

### Documentation

- Add module docstring
  ([`81d40a2`](https://github.com/bec-project/bec/commit/81d40a233148a34ab7fa71c16afc7ab361632e36))

- Fix typo
  ([`77f4072`](https://github.com/bec-project/bec/commit/77f407233421bd4838e8d22f53b3342cd67e47e1))

### Refactoring

- Cleaned up scan core and removed hard coded speec/acc from stages
  ([`ccb5b03`](https://github.com/bec-project/bec/commit/ccb5b03add6424085abaefbc941b1bf160a16296))

- Finished refactoring of scan
  ([`dfe8f1b`](https://github.com/bec-project/bec/commit/dfe8f1ba1a0927464eebe5131acc32bf980a0805))

- Fix docstring
  ([`4e9eaad`](https://github.com/bec-project/bec/commit/4e9eaad4ac5c2a815898b76acd3cbbb2a5ec7338))

- Move velo and acc calc. in separate function and call it from scan_core
  ([`7c3ed51`](https://github.com/bec-project/bec/commit/7c3ed5198cd7c8345d7066999867ac7e2d55efb7))

### Testing

- Add test to owis_grid
  ([`830d70d`](https://github.com/bec-project/bec/commit/830d70d7fb91244e7f9d1fd912a6aae1c373cc21))

- Remove redundant lines
  ([`afc2cc7`](https://github.com/bec-project/bec/commit/afc2cc7fe7dd9e7bad8d89ce9c471d7eb6fe1280))


## v0.42.8 (2023-11-18)

### Bug Fixes

- Added status eq dunder
  ([`f1327d4`](https://github.com/bec-project/bec/commit/f1327d409117f91f17917d6fe30a1dae8e4cbb90))

- Fixed ctrl c behaviour for rpc calls; closes #119
  ([`9986a72`](https://github.com/bec-project/bec/commit/9986a7292629668b6f398bee411bada04b535adc))

### Testing

- Refactored test to use dev fixture instead of bec_client
  ([`fc39c4a`](https://github.com/bec-project/bec/commit/fc39c4ad1eae3cdaf6cea75618a55db9649f8779))


## v0.42.7 (2023-11-18)

### Bug Fixes

- Fixed signature serializer for py >3.9
  ([`6281716`](https://github.com/bec-project/bec/commit/6281716b2974a7b074aa4b6ef465427f3603937e))

- Fixed signature serializer for typing.Literal
  ([`5d4cd1c`](https://github.com/bec-project/bec/commit/5d4cd1c1918b4f417b9ebb51e5a12b5692bd7384))

### Refactoring

- Added type hints for optim_trajectory; closes #117
  ([`9689606`](https://github.com/bec-project/bec/commit/968960646cfe5d19a327a63c33be60a32ab0752c))

- Improved pylint score
  ([`fb1c015`](https://github.com/bec-project/bec/commit/fb1c0154a5be7171a33a32d1ec698915a119ca7f))

- Updated bec_lib import to new bec_lib structure
  ([`c47a46f`](https://github.com/bec-project/bec/commit/c47a46f049c36b224c9bee7977e0d1a7a2e43936))


## v0.42.6 (2023-11-18)

### Bug Fixes

- Include all needed files in packaged distro
  ([`2b3eddc`](https://github.com/bec-project/bec/commit/2b3eddcff62d3a8085f2f8d1a5826020ecd87107))

Fix #118


## v0.42.5 (2023-11-17)

### Bug Fixes

- Fixed creation of nested device components; needed for DynamicComponents
  ([`407f990`](https://github.com/bec-project/bec/commit/407f99049091f78efc3b8fac6bb7046cc0a6b623))

### Testing

- Added dm client test for nested signals
  ([`fe11076`](https://github.com/bec-project/bec/commit/fe110764c26a68661ff06633f94cdcebfda6352a))

- Added test for dyn device components
  ([`0f3ab89`](https://github.com/bec-project/bec/commit/0f3ab898757af672149deaf8c374436fd0824476))


## v0.42.4 (2023-11-17)

### Bug Fixes

- Removed redundant name in config output
  ([`5a81c21`](https://github.com/bec-project/bec/commit/5a81c2134593b702fcd6f2645e952caa7cdaf2d2))

### Continuous Integration

- Added pylint-check to .gitlab-ci.yml
  ([`e33ce81`](https://github.com/bec-project/bec/commit/e33ce811413be6bfcd67a1fb06f4f0265eebc921))

- Fixed pylint test
  ([`a9bbece`](https://github.com/bec-project/bec/commit/a9bbecec011cf4c780f7f485c35bb8893d4a57c2))

- Improved pylint check
  ([`972c544`](https://github.com/bec-project/bec/commit/972c544f3071268a784dce14fb316c8b8fcdaea2))

- Made pylint check optional
  ([`b6aca18`](https://github.com/bec-project/bec/commit/b6aca18455a30c1d02c61e90bb1862dcb2739471))

- Testing pylint
  ([`b1bbee1`](https://github.com/bec-project/bec/commit/b1bbee16ed6a9169912c27e7ec7a1b1fed13f342))

- Testing pylint_check
  ([`087c5a3`](https://github.com/bec-project/bec/commit/087c5a30984b2cde5363a5390446f1a9d960575f))

- Testing pylint_check
  ([`55648d3`](https://github.com/bec-project/bec/commit/55648d3cc89a447880f608f9711c90724555fbd7))

- Testing pylint_check
  ([`d162f84`](https://github.com/bec-project/bec/commit/d162f8414f44556b3efce41f11c73166300ff6e6))

- Testing pylint_check
  ([`e0166d4`](https://github.com/bec-project/bec/commit/e0166d40f6d5ebb6df98e3e49622868fb669c245))

- Testing pylint_check
  ([`e1074dd`](https://github.com/bec-project/bec/commit/e1074dd1a005abc409ac38f5ff377e1a87ba0873))

- Updated pylint check to handle floating point comparison"
  ([`10bebbb`](https://github.com/bec-project/bec/commit/10bebbb6fbc6b58522ccaa758d44b174e77942f4))

### Refactoring

- Improved setup.py
  ([`45b6a93`](https://github.com/bec-project/bec/commit/45b6a937b7333f1d96fee14a22550b51cd3f9be2))


## v0.42.3 (2023-11-12)

### Bug Fixes

- Added missing init file
  ([`109453c`](https://github.com/bec-project/bec/commit/109453c1ccb3ebc8506e57f549549f99b38e4c8f))


## v0.42.2 (2023-11-10)

### Bug Fixes

- Bec_service test
  ([`97d3d1f`](https://github.com/bec-project/bec/commit/97d3d1f18f07101a860952f40a96b7cfd633fb3c))

- Resolve a circular import in logbook_connector
  ([`8efd02c`](https://github.com/bec-project/bec/commit/8efd02cda483aaa29cdb6bbb9867a67037a25111))

### Refactoring

- Flatten bec_lib structure
  ([`524ef24`](https://github.com/bec-project/bec/commit/524ef24da05fce33ad09e420f669fb50684af139))

- Major scan worker refactoring; added separate device validation mixin
  ([`0c7ae79`](https://github.com/bec-project/bec/commit/0c7ae795c41ba5f71b315da2d3a7c7895a0cf74c))

- Make all type checking imports conditional
  ([`cc9227f`](https://github.com/bec-project/bec/commit/cc9227f893e63fc594e5236414250f6960e096f4))

- Make bec_lib imports absolute
  ([`7166c8b`](https://github.com/bec-project/bec/commit/7166c8b5eef21a9146f5d5d78c6a83e3ddf8f03f))

- Move Alarms enum into alarms_handler
  ([`943b10d`](https://github.com/bec-project/bec/commit/943b10dac0f9e21bd42145486552a8fb26ee6d11))

- Move bec_lib test utils out of core folder
  ([`de5da55`](https://github.com/bec-project/bec/commit/de5da559a651450bcab8ca40f553ca60ab27845f))

- Move scripts into util_scripts folder
  ([`bbd0eee`](https://github.com/bec-project/bec/commit/bbd0eee6bb04447456869304f5df1a1926ad7477))

- Remove session_manager and singleton_threadpool
  ([`d54453d`](https://github.com/bec-project/bec/commit/d54453d7e7120555ae97584572ccc834d5050379))

- Remove unused imports in bec_lib
  ([`171a5c6`](https://github.com/bec-project/bec/commit/171a5c6609da76b3f17848ba01b50806e389255c))

- Rename module BECMessage -> messages
  ([`06f2d78`](https://github.com/bec-project/bec/commit/06f2d781ae445a3e03afce821b2f732d7f6e3f90))

This should help to avoid confusion between BECMessage module and BECMessage class located in the
  same module

- Run isort on all files
  ([`146898e`](https://github.com/bec-project/bec/commit/146898ec3fd56e2fdf6df49999be80ab4778b618))

$ isort . --profile=black --line-width=100 --multi-line=3 --trailing-comma

- Temporarily add module stubs
  ([`108dc11`](https://github.com/bec-project/bec/commit/108dc1179cf191c98b7891605d665371b5a2bca2))

- Warn about importing from old module paths
  ([`b73e9b3`](https://github.com/bec-project/bec/commit/b73e9b3baac660bb7af3fc049c27e3bdb294bba9))

### Testing

- Fixed test for refactored worker
  ([`2da7dd3`](https://github.com/bec-project/bec/commit/2da7dd385f1d7edbb9408a3af2edf458b7f4cea2))


## v0.42.1 (2023-11-09)

### Bug Fixes

- Fixed bec service update routine for missing messages; closes #114
  ([`dc37058`](https://github.com/bec-project/bec/commit/dc370584c9265b4fc28e79bd2bd9609c826668f8))

### Continuous Integration

- Changed default order
  ([`53954d3`](https://github.com/bec-project/bec/commit/53954d3bdc149d73d1cf013875dd269aa9d95cb9))

- Disabled scibec end2end for now; re-enable once scibec is running
  ([`93ab2c3`](https://github.com/bec-project/bec/commit/93ab2c37e106d86640d53fcdd8b8107e6901296b))


## v0.42.0 (2023-11-07)

### Features

- Added scan base class to scan info
  ([`5ecc189`](https://github.com/bec-project/bec/commit/5ecc1893439c46578b9da48913f80ff72d7b1fb9))

### Refactoring

- Added scancomponent base class
  ([`41206ef`](https://github.com/bec-project/bec/commit/41206efc1395fb3d6ace3bf9799876aab453a73a))


## v0.41.0 (2023-11-06)

### Bug Fixes

- Fixed scan signature for scan defs and group def
  ([`3589e3e`](https://github.com/bec-project/bec/commit/3589e3e36fc4222592dcf9912a9be45b8cc91eea))

### Features

- Changed arg_bundle_size from int to dict; closes #111
  ([`1a8cc7c`](https://github.com/bec-project/bec/commit/1a8cc7c448edd3f712cf2fc20070abefad69dd66))


## v0.40.0 (2023-11-06)

### Features

- Added log to report on missing device status updates
  ([`261497a`](https://github.com/bec-project/bec/commit/261497ad985bd52ab9db38086cd5421bc03331d2))


## v0.39.0 (2023-11-02)

### Bug Fixes

- Added missing type hints to scan signatures
  ([`6b21908`](https://github.com/bec-project/bec/commit/6b2190899d16d5bc1b1a582ca5f7159f1be6a56d))

- Removed helper plugin
  ([`87100ca`](https://github.com/bec-project/bec/commit/87100caaf07851ce758dfa5e42f5c121eff2b886))

### Features

- Changed arg_input from list to dict to provide a full signature
  ([`c7d8b1a`](https://github.com/bec-project/bec/commit/c7d8b1afd510cbb63f097b74121bb1b7b9e89ffc))


## v0.38.1 (2023-11-02)

### Bug Fixes

- Fixed nested get for missing fields
  ([`9be82f1`](https://github.com/bec-project/bec/commit/9be82f12c6b42e99c61eeacc6185c663f95c9ab6))

### Testing

- Added test for nested get
  ([`f5fddaf`](https://github.com/bec-project/bec/commit/f5fddafdf3fe33b3382180a03601f37dfe7f67ef))


## v0.38.0 (2023-11-01)

### Features

- Added config option to abort on ctrl_c; closes #95
  ([`705daa6`](https://github.com/bec-project/bec/commit/705daa6d9e9642fdea85adafa12e4946e69bcd6c))


## v0.37.0 (2023-11-01)

### Bug Fixes

- Fixed readout_priority update
  ([`aee1bda`](https://github.com/bec-project/bec/commit/aee1bdae1461dd1bb8c0f959c8bce97605074d9d))

### Features

- Added option to specify monitored devices per scan; closes #100
  ([`d3da613`](https://github.com/bec-project/bec/commit/d3da613bfdf3721f5c52f5491bf64b01317a4126))

### Testing

- Fixed test for monitored devices
  ([`c20a6b2`](https://github.com/bec-project/bec/commit/c20a6b2d9d44d68e71f4e678c31b472709a9c142))


## v0.36.3 (2023-11-01)

### Bug Fixes

- Added missing timestamp to flyer update
  ([`091df2f`](https://github.com/bec-project/bec/commit/091df2f0a136a78423159faa35308d44f68f535c))


## v0.36.2 (2023-10-31)

### Bug Fixes

- Added device name to flyer readout
  ([`cd82727`](https://github.com/bec-project/bec/commit/cd827271bb738ced288450dc79b7dc0316e6b0b9))

- Fixed error that caused the scan worker to shut down instead of raising for scan abortion
  ([`f1e8bfb`](https://github.com/bec-project/bec/commit/f1e8bfba80468dc9aa7d057ecb57ef383c215c71))

### Refactoring

- Reduced log level for completing a device
  ([`221478c`](https://github.com/bec-project/bec/commit/221478c15cdd85e6f8532c2c7f642afd7ec02de0))


## v0.36.1 (2023-10-30)

### Bug Fixes

- Add '.[dev]' to bash scripts to avoid escape char in certain shells while install
  ([`0d5168d`](https://github.com/bec-project/bec/commit/0d5168dcedf32902ffd866c45d85457c4f22e7e7))


## v0.36.0 (2023-10-30)

### Bug Fixes

- Fixed bug in complete for all devices
  ([`08d34a8`](https://github.com/bec-project/bec/commit/08d34a8418b768209b0721ac876b76575699ae7e))

### Documentation

- Updated introduction; added scripts and scan defs
  ([`b9f2eab`](https://github.com/bec-project/bec/commit/b9f2eab7297fd38085d1d77e0dd66aa070fe051e))

### Features

- Added complete call to all devices; closes #93
  ([`042e51e`](https://github.com/bec-project/bec/commit/042e51e857cad3198823c9227e593b15ba1a233f))

### Refactoring

- Cleanup
  ([`dfc0abe`](https://github.com/bec-project/bec/commit/dfc0abe7b6c7b6aaab2b637460f8776dc5153419))

- Cleanup
  ([`cc5f5ac`](https://github.com/bec-project/bec/commit/cc5f5ac3a26d7f9bde55b359c06cd6375a37d37a))

- Merged wait for stage
  ([`2b9d39d`](https://github.com/bec-project/bec/commit/2b9d39da73acbb5659a4919fef35607e6a674639))

### Testing

- Added test for complete with list of devices
  ([`ec016e4`](https://github.com/bec-project/bec/commit/ec016e4466ec25ddc7f68ee5597fd7f285d086e9))


## v0.35.1 (2023-10-06)

### Bug Fixes

- Changed progress update from devicestatus to progress message
  ([`03595b4`](https://github.com/bec-project/bec/commit/03595b42f78f45f2c5d2e7bf10e860a3ee5297d4))

### Testing

- Fixed test for new progress messages
  ([`1190461`](https://github.com/bec-project/bec/commit/1190461c82d28dbe23fdc3a245dfcf2e754eb248))


## v0.35.0 (2023-10-06)

### Bug Fixes

- Added missing pre scan to acquire
  ([`d746093`](https://github.com/bec-project/bec/commit/d7460938041dac4be45fa39cbdaa957dda5f88ca))

- Adjusted sgalil_grid scan for updated mcs operation
  ([`b7a722c`](https://github.com/bec-project/bec/commit/b7a722c0ef4ce14fd6843c368bf076bd1024db23))

- Enabled scilog
  ([`64e82c6`](https://github.com/bec-project/bec/commit/64e82c67782a701f5eeb04a3a9c1ce42832c1fdf))

- Fixed bl_check repeat
  ([`62aa0ae`](https://github.com/bec-project/bec/commit/62aa0aed78c23ea0c117f84ba54d5f267f35eed4))

- Fixed primary readout for sgalil scan
  ([`4231d00`](https://github.com/bec-project/bec/commit/4231d00e19aacc039ee9cc9f4a1f18294ea18ab0))

- Fixed scan bundler for async fly scans
  ([`d7a6b0f`](https://github.com/bec-project/bec/commit/d7a6b0fee010877dfd18cbf23ff55126a399dec9))

- Fixed scan progress for messages without scanID
  ([`64f3b13`](https://github.com/bec-project/bec/commit/64f3b13e9710dbfb207c11fbd683db9cb9462dda))

- Fixed stage instruction for detectors
  ([`ac7a386`](https://github.com/bec-project/bec/commit/ac7a386acf62d381ad096d816da6db30bcfa5ce7))

- Fixed tmux launch for mono environments
  ([`5be5dda`](https://github.com/bec-project/bec/commit/5be5dda1cfd3adfadae5e314a8ba87b394e8227a))

- Online changes e20643, file writer bugfix and add scanabortion check in sgalil_grid
  ([`195a8dd`](https://github.com/bec-project/bec/commit/195a8dd7541fa12ff5f4d9c9cc0a70651af805b1))

- Optimize staging of devices in scanserver and device server
  ([`2c66dbb`](https://github.com/bec-project/bec/commit/2c66dbbe4667120195151f49acfe5b45527e21b9))

- Sgalil scan corrections
  ([`2f8fce5`](https://github.com/bec-project/bec/commit/2f8fce52207fdbab2a5db562ca7cfa3beb814e41))

### Features

- Grid fly scan with standard epics owis motors
  ([`552aff5`](https://github.com/bec-project/bec/commit/552aff5bd9fd0bb61e3f50133d4bbf52cc824857))

### Refactoring

- Cleanup
  ([`a3b70e5`](https://github.com/bec-project/bec/commit/a3b70e5b61f42cf3cfc582c044f193d5bdd27219))

- Cleanup
  ([`766b1bd`](https://github.com/bec-project/bec/commit/766b1bd06f6fd624c17f56680d99901cf3d70501))

- Fix formatting for online changes
  ([`7232aef`](https://github.com/bec-project/bec/commit/7232aeffc8fbfa053685aeb9d156e50080976da1))

- Fixed formatter
  ([`b7ad6fe`](https://github.com/bec-project/bec/commit/b7ad6fefc778c8e417e67aa49c336698d5fc428f))


## v0.34.2 (2023-10-05)

### Bug Fixes

- Fixed bug for aborted scans
  ([`e7d73e5`](https://github.com/bec-project/bec/commit/e7d73e5b2b2cccb829e401b209605523f6b7dbce))


## v0.34.1 (2023-10-02)

### Bug Fixes

- Write files on abort and halt
  ([`910a92f`](https://github.com/bec-project/bec/commit/910a92f4784c93119e63c9abad24ec1315718a45))


## v0.34.0 (2023-09-07)

### Bug Fixes

- Added missing primary readings to sgalil grid
  ([`e52390a`](https://github.com/bec-project/bec/commit/e52390a2269370f6806f93896234bc076a0731f4))

### Features

- Added progress endpoint and message
  ([`ad60b78`](https://github.com/bec-project/bec/commit/ad60b7821a1de36645e2c70023ea73bb7d141e39))


## v0.33.0 (2023-09-07)

### Bug Fixes

- Add eiger9m to cSAXS nexus file writer plugin
  ([`8ba441f`](https://github.com/bec-project/bec/commit/8ba441f55fdb9659aff12d2535799f268af1d815))

- Add eiger9m to cSAXS nexus file writer plugin
  ([`375150c`](https://github.com/bec-project/bec/commit/375150ce58e00f2b6f53d713ac35cebdb087b6ad))

- Add file_writer plugin cSAXS and file_event for new file from device
  ([`b1f4fcc`](https://github.com/bec-project/bec/commit/b1f4fccaaaec9cded2182554900ca48ceeb2fdc3))

- Add file_writer plugin cSAXS and file_event for new file from device
  ([`0fdf164`](https://github.com/bec-project/bec/commit/0fdf1647aaf153d480f952ff1515fda2a1a1640d))

- Add frames_per_trigger to scans and scan server
  ([`51c8a54`](https://github.com/bec-project/bec/commit/51c8a54f01c6b5a0a09c90cb5a21e5640b3cd884))

- Add frames_per_trigger to scans and scan server
  ([`0c66dc3`](https://github.com/bec-project/bec/commit/0c66dc33593379c7e2bee8499af8d6cecf32b761))

- File_writer and scan_ser for falcon and eiger9m and sgalil grid scan
  ([`cec0b34`](https://github.com/bec-project/bec/commit/cec0b342f0c518bb37c4403cb55336792a192cec))

- Online fix for file writer
  ([`de5ba09`](https://github.com/bec-project/bec/commit/de5ba09954468bf696e2aa27f00532fe7780ef27))

### Features

- Add sgalilg_grid to scan_plugins and make scantype flyscan scan possible
  ([`a5ba186`](https://github.com/bec-project/bec/commit/a5ba186ad14283fae7c5160180a759e29f78137d))

### Refactoring

- Cleanup after merge
  ([`f528612`](https://github.com/bec-project/bec/commit/f5286126ff0203c34bb57799080decfd1ffeb0d5))

### Testing

- Fixed tests
  ([`31fca80`](https://github.com/bec-project/bec/commit/31fca804cd14b2916241117e9b595118fd2abc9c))


## v0.32.0 (2023-09-06)

### Bug Fixes

- Removed pre move from fly scan
  ([`ed095b0`](https://github.com/bec-project/bec/commit/ed095b00cbebc50ebecaabc696b8aaf4a728270d))

- Removed pre move from fly scan
  ([`f8ad2f8`](https://github.com/bec-project/bec/commit/f8ad2f8a2781fa38000c29b39772132eaa63e4ce))

### Documentation

- Added premove and enforce_sync doc
  ([`fd38985`](https://github.com/bec-project/bec/commit/fd38985767ead15678f45ac60d0ee59bb8ee8df6))

### Features

- Added pre_scan
  ([`7f23482`](https://github.com/bec-project/bec/commit/7f23482b5cf273f06776e497783f44361a2cb58f))


## v0.31.0 (2023-09-05)

### Features

- Added support for loading the service config from plugins
  ([`f3d3679`](https://github.com/bec-project/bec/commit/f3d3679e216492d8dfaf35ff00f75520652863fc))

### Testing

- Fixed test for trigger
  ([`23af7b5`](https://github.com/bec-project/bec/commit/23af7b52c62cd42242c5ec931101cbaf20dd2573))

- Fixed test for trigger
  ([`54ba69b`](https://github.com/bec-project/bec/commit/54ba69b9834d602bda0ec7f3006bced4371f7b40))


## v0.30.1 (2023-09-05)

### Bug Fixes

- Added sleep before polling for status
  ([`c8acaa4`](https://github.com/bec-project/bec/commit/c8acaa4b71504a8b34c9f05f4ef6af5ab444a424))

- Removed hard-coded trigger wait; waiting for status instead
  ([`086c863`](https://github.com/bec-project/bec/commit/086c8634e30baf4ae1b74ae61bd3f8070c69d320))

### Testing

- Fixed test for new wait
  ([`9aefe83`](https://github.com/bec-project/bec/commit/9aefe83b02b3b99d1633bd48ffd701fd2cbfaf2b))


## v0.30.0 (2023-09-04)

### Features

- Added preliminary version of bl_checks
  ([`bfa1d67`](https://github.com/bec-project/bec/commit/bfa1d678735cc8dcfb303446517254290c7c7921))

- Beamline check
  ([`cae5f61`](https://github.com/bec-project/bec/commit/cae5f61924744d0358527b074958bdfe102bb2cd))

### Refactoring

- Refactored beamline checks to simplify unit tests
  ([`e9d0fbe`](https://github.com/bec-project/bec/commit/e9d0fbe44403013430380bd1a8f98d951bbb25be))


## v0.29.0 (2023-09-04)

### Bug Fixes

- Fixed signal init
  ([`41282e5`](https://github.com/bec-project/bec/commit/41282e57678d6a39a1f40fdf828e2fdb2ddc0193))

### Features

- Added bec_plugins as source for devices
  ([`bbcdbc0`](https://github.com/bec-project/bec/commit/bbcdbc0123566f4bea811fb9c873e059b4eb4a7c))


## v0.28.0 (2023-09-02)

### Bug Fixes

- Fixed scan_progress import
  ([`5eda477`](https://github.com/bec-project/bec/commit/5eda477723d4dfc0387e0293713ef8e197a58f53))

- Ipython client should use default service config
  ([`9b89aec`](https://github.com/bec-project/bec/commit/9b89aecfdc0449a9d40aae642dccf2408989c6d1))

### Features

- Added progress bar based on async devices
  ([`11e5f96`](https://github.com/bec-project/bec/commit/11e5f96b7575e0a811f45914e99ada6d2c449648))

- Added scan progress
  ([`9f6a044`](https://github.com/bec-project/bec/commit/9f6a044fe316c804e2e4dfc34435c9eb71cd109b))

- Added xrange
  ([`f4f38d6`](https://github.com/bec-project/bec/commit/f4f38d6deab2026177126e58cf1eac20490d9942))

### Refactoring

- Removed code duplication
  ([`d0bc94b`](https://github.com/bec-project/bec/commit/d0bc94b671c4ce2851ac7d3c9c3a209b79a6cb36))

### Testing

- Added more file_manager tests
  ([`54517ca`](https://github.com/bec-project/bec/commit/54517cabef2158bc33dcd7e30cb7de352c0a94bc))

- Added more file_manager tests
  ([`153c38a`](https://github.com/bec-project/bec/commit/153c38aea6c9f47123b47655fb1d20ff4b79fa1c))

- Added xrange and get_last tests
  ([`1a24616`](https://github.com/bec-project/bec/commit/1a24616cb3b2e8a44860a8e34f9294e635449505))


## v0.27.0 (2023-08-31)

### Features

- Added get_last; changed streams to stream suffix
  ([`e84601f`](https://github.com/bec-project/bec/commit/e84601f487d4943c63a31f12b42d656dc9a4c690))


## v0.26.0 (2023-08-31)

### Bug Fixes

- Adjust xadd to allow streams to expire
  ([`33fbded`](https://github.com/bec-project/bec/commit/33fbdedd3eed52ded4eb53043bc7407997d51e4a))

- Bugfix
  ([`57c989c`](https://github.com/bec-project/bec/commit/57c989cfe204a657bcefac2364a6a0ad98a77ff1))

- Fixed xadd for pipelines
  ([`d19fce7`](https://github.com/bec-project/bec/commit/d19fce7d21a12eac2f8ac9b083fff464e5d0da9e))

- Online changes
  ([`9b07e0f`](https://github.com/bec-project/bec/commit/9b07e0f8a2d774a9a6a07ab9faa9167585532dcd))

### Features

- Add new endpoint for async device readback
  ([`5535797`](https://github.com/bec-project/bec/commit/5535797e1e25121d7a3997d78aa6c43eff17e086))

### Testing

- Added test for obj destruction
  ([`79c9e3c`](https://github.com/bec-project/bec/commit/79c9e3c841c888680df4f0ed6e65f4f20cda359d))


## v0.25.0 (2023-08-31)

### Features

- Added support for startup scripts from plugins
  ([`d35caf5`](https://github.com/bec-project/bec/commit/d35caf5ae40b5b46f3b2adad139cad66b3091857))


## v0.24.0 (2023-08-31)

### Bug Fixes

- Fixed worker manager
  ([`fa62a8a`](https://github.com/bec-project/bec/commit/fa62a8a9c96da44439ba71ae82d8020c8a2a0de5))

### Continuous Integration

- Removed repo updates
  ([`8ba01d8`](https://github.com/bec-project/bec/commit/8ba01d84fa7b9c7661e0fb742aa9cd9193926625))

### Features

- Added available resource endpoint/message
  ([`5f5c80c`](https://github.com/bec-project/bec/commit/5f5c80c2866236226dca717de0c67b32f5692ab9))

- Added global var service config to simplify sharing the config with other classes
  ([`75f1f9c`](https://github.com/bec-project/bec/commit/75f1f9cd4ebc6938f2cf47103fb64eef8be57ae3))

- Added option to update the worker config directly
  ([`a417fd8`](https://github.com/bec-project/bec/commit/a417fd8a18cadb4b480da243149c1186f3a07d88))


## v0.23.1 (2023-08-31)

### Bug Fixes

- Removed bec prefix from file path
  ([`9a3b20f`](https://github.com/bec-project/bec/commit/9a3b20f085232369c9320bb8f54b93fb6b0b1686))

### Testing

- Fixed test for new base path
  ([`3c55393`](https://github.com/bec-project/bec/commit/3c553933ac4350300c8ece72c13a6f741216d760))


## v0.23.0 (2023-08-29)

### Bug Fixes

- Fixed live table for hinted signals
  ([`4334567`](https://github.com/bec-project/bec/commit/43345676533a402fac517fd467c98b46f35658aa))

### Features

- Added device precision
  ([`4177fe6`](https://github.com/bec-project/bec/commit/4177fe6038a10e2f285fc18c13ef6a77022b17e5))

- Added support for user scripts from plugins and home directory
  ([`cd59267`](https://github.com/bec-project/bec/commit/cd59267e780586b002cd80c692a0f38c213f999d))

### Refactoring

- Fixed formatter
  ([`0446717`](https://github.com/bec-project/bec/commit/044671730ed31916a5936f2b5bd104c65db081d0))

- Fixed formatting
  ([`dbca77e`](https://github.com/bec-project/bec/commit/dbca77ec5e46d3498a5a82bd4202082ce904c35c))

### Testing

- Added tests for live feedback
  ([`aa2d685`](https://github.com/bec-project/bec/commit/aa2d68535e7f30e489a7e2735a7cd472c4973335))


## v0.22.0 (2023-08-24)

### Features

- Added acquisition config and readout_time
  ([`f631759`](https://github.com/bec-project/bec/commit/f63175941bbf7d9f5448ff58b9ea942bd2e1b9a4))


## v0.21.1 (2023-08-21)

### Bug Fixes

- Fixed bug in device config update
  ([`940737f`](https://github.com/bec-project/bec/commit/940737fe6c8295423390a76b784a5984a93c7043))

### Refactoring

- Removed outdated/unused monitor_devices
  ([`73c0348`](https://github.com/bec-project/bec/commit/73c03484fb7b7d1b2b5ed7bb40dd13bc31c38496))

### Testing

- Fixed test
  ([`9aeadff`](https://github.com/bec-project/bec/commit/9aeadff27a63edf338a65412a398d9ff3223c9fe))


## v0.21.0 (2023-08-20)

### Features

- Inject device_manager based on signature
  ([`4eb9cf4`](https://github.com/bec-project/bec/commit/4eb9cf494c805cdf751e459f0b9d0b7aa3ebee91))


## v0.20.0 (2023-08-20)

### Bug Fixes

- Fixed interceptions for multiple queues
  ([`4e5d0da`](https://github.com/bec-project/bec/commit/4e5d0da38b06f11e6abe5ce23687cdf237c9ffeb))

- Removed primary queue from init; cleanup
  ([`bb04271`](https://github.com/bec-project/bec/commit/bb042716fecbc3035483184e494e9e4f3d2d82da))

### Features

- Added device precision to rpc base class
  ([`2c7b55f`](https://github.com/bec-project/bec/commit/2c7b55f828f3f68ff05095a007724e499797126b))

- Added option to specify thread names
  ([`cae0ba2`](https://github.com/bec-project/bec/commit/cae0ba2d3ea659a7de3936acdc257e1aa0991311))

- Added support for multiple queues; still WIP
  ([`9019cc2`](https://github.com/bec-project/bec/commit/9019cc2c7443c38c47160af843eef7e3f070a25b))

### Refactoring

- Added thread names
  ([`746cff2`](https://github.com/bec-project/bec/commit/746cff26dd156566d15a0678db28fddce720492f))

- Renamed primary to monitored
  ([`863dbc8`](https://github.com/bec-project/bec/commit/863dbc86bbd6c53052c9066e65bd64f7c02dbaa8))

- Renamed stream to readout_priority
  ([`1802e29`](https://github.com/bec-project/bec/commit/1802e29b3d3dd4d00b89c6ea66881ab8143e15c7))


## v0.19.0 (2023-08-20)

### Bug Fixes

- Fixed dap worker for plugins
  ([`e2f3d8f`](https://github.com/bec-project/bec/commit/e2f3d8f29ddc771798d0e2cc43f7f0d85db00fe9))

- Remove parameters for saxs_imaging_processor
  ([`39c7a9c`](https://github.com/bec-project/bec/commit/39c7a9c0be0a0b9861961e5443f313e11fb35748))

### Features

- Add bec_worker_manager.py
  ([`f0ba36d`](https://github.com/bec-project/bec/commit/f0ba36db869b8a0e06918ef1fd9fc44a87cbd217))

- Added dap to client
  ([`0ea549a`](https://github.com/bec-project/bec/commit/0ea549a599f4ac3dccffe7fa2f148e48a0c5d7c1))

- Pluging support for data_processing
  ([`9e33418`](https://github.com/bec-project/bec/commit/9e334185260e5f92964e1f3f5b5d6d3a86d4c1d6))

### Testing

- Added logger output
  ([`11d7b41`](https://github.com/bec-project/bec/commit/11d7b411c0466ab12f311a5c0fe9fb954a79f238))

- Added missing tests
  ([`30b3cb1`](https://github.com/bec-project/bec/commit/30b3cb18ff70ff3db93a7fc585a1287df905ceb3))

- Ensure that values are beyond limits
  ([`7c5c3ff`](https://github.com/bec-project/bec/commit/7c5c3ff73599b4cdb95d68b5c442214ac5ca6204))


## v0.18.1 (2023-08-19)

### Bug Fixes

- Fixed bug in wait function for aborted move commands
  ([`019fcda`](https://github.com/bec-project/bec/commit/019fcdaa074dcb67c84132cb038067dca8578830))

- Removed timeout
  ([`29df4ac`](https://github.com/bec-project/bec/commit/29df4ac19ac189f4d7666c2c47c4539cf5e94372))

### Continuous Integration

- Added dummy functional account
  ([`c468a7a`](https://github.com/bec-project/bec/commit/c468a7ab18d3c676628ed14826148e073633d750))

- Disabled end2end test with API server for now until the server is back in operation
  ([`9303171`](https://github.com/bec-project/bec/commit/930317156cb09e9473eff36d875ae4252176bb7b))

- Fixed path to explorer
  ([`3a319e2`](https://github.com/bec-project/bec/commit/3a319e2006ef1b0edcc16a606e1dcefb08d2b505))

- Fixed path to openapi file
  ([`6bb20c2`](https://github.com/bec-project/bec/commit/6bb20c2d2d64ffa967a21695ffe33478a7a0e9ab))

### Refactoring

- Improved scan report
  ([`2ebe440`](https://github.com/bec-project/bec/commit/2ebe44053523ed14b69552daf63fa088e2ee0e1a))

- Removed outdated timeout function
  ([`caf64e3`](https://github.com/bec-project/bec/commit/caf64e31bc603b4df3bd84d8101d671de8a75b36))

### Testing

- Fixed import
  ([`72b61d0`](https://github.com/bec-project/bec/commit/72b61d05bf31f6164dd7fb015217cc43a364fcc7))


## v0.18.0 (2023-08-15)

### Bug Fixes

- Added missing file
  ([`f55a518`](https://github.com/bec-project/bec/commit/f55a518b9103f93b54c872fb4387956cb783d5b8))

- Fixed bug in unpack_scan_args for empty lists
  ([`a693f84`](https://github.com/bec-project/bec/commit/a693f84816d9074a3f4664a8530d0b130702f7a2))

- Fixed typo in round_roi_scan init; added test
  ([`75f2217`](https://github.com/bec-project/bec/commit/75f221758f939c510a7766101cc3faa0250a0b6b))

### Continuous Integration

- Allow repo update to fail
  ([`e2b9a5e`](https://github.com/bec-project/bec/commit/e2b9a5e76f9d5324198f0bba4b340a54c80d2783))

### Features

- Scan signature is now exported; simplified scan init
  ([`f35b04a`](https://github.com/bec-project/bec/commit/f35b04a676a8c6aa972f031d83cb637b346d5d4f))


## v0.17.2 (2023-08-10)

### Bug Fixes

- Added MessageObject eq dunder
  ([`563c628`](https://github.com/bec-project/bec/commit/563c6285092b9d8e33e8c93dea95986b87f5c67a))

### Continuous Integration

- Added workflow to avoid detached pipelines
  ([`26eb77d`](https://github.com/bec-project/bec/commit/26eb77dbfdddc1b018fa571166d9622ec73c0036))

### Refactoring

- Moved trim to separate function
  ([`dc954e2`](https://github.com/bec-project/bec/commit/dc954e2b3c380b665d695cf5616480ae51dec2c1))

### Testing

- Added redis connector tests
  ([`2f30c64`](https://github.com/bec-project/bec/commit/2f30c64cd76f15b81b9f128d9a5562f35be85a8a))


## v0.17.1 (2023-08-10)

### Bug Fixes

- Fixed default config
  ([`8ad8d84`](https://github.com/bec-project/bec/commit/8ad8d84e00a62306d43862192c8a16b09e17a17b))

### Refactoring

- Unified service configs
  ([`89d4bb3`](https://github.com/bec-project/bec/commit/89d4bb37c3d30b5c0649b524a9eca6f7bdba8a19))


## v0.17.0 (2023-08-10)

### Bug Fixes

- Fixed bec_service if service keys are not available
  ([`9b71f77`](https://github.com/bec-project/bec/commit/9b71f77dacf0fe1313fe6f0c1e9de73572286b96))

- Fixed scan number if redis is not available
  ([`8514d2d`](https://github.com/bec-project/bec/commit/8514d2d6384516f53fd75d4ef671e24f32fad0f4))

- Fixed scans if redis is not available
  ([`b0467a8`](https://github.com/bec-project/bec/commit/b0467a86aaf4741484ef0fb66e6441e742142cb5))

### Build System

- Added bec rtd
  ([`495a2bb`](https://github.com/bec-project/bec/commit/495a2bb3a88c4b521225aff43ef310a24ec8fbd9))

### Features

- Added stream consumer
  ([`b4043e9`](https://github.com/bec-project/bec/commit/b4043e970ac0d3fe2bbd6cb8d386967aefcf812d))

### Refactoring

- Reverted to decorators with warning
  ([`dc0e61b`](https://github.com/bec-project/bec/commit/dc0e61be09b918df3aecdcbc4cdd98067510eec5))


## v0.16.3 (2023-08-06)

### Bug Fixes

- Catch redis connection errors
  ([`31efa96`](https://github.com/bec-project/bec/commit/31efa96cec20540a00f0be199e8fda4fa04fdc68))

- Fixed default arg for initialize
  ([`b65aba8`](https://github.com/bec-project/bec/commit/b65aba8a5fcdb8f2f5eeb488725144f46267f074))

- Scanbundler sets status to running
  ([`d0d46ba`](https://github.com/bec-project/bec/commit/d0d46ba76b1351f5431d7c93a6d6591c250563d7))

- Wait for bec server should only be done for ipython, not the bec lib
  ([`9dfe389`](https://github.com/bec-project/bec/commit/9dfe38943f2b8d6be051612de9f31ad8171f1073))

### Documentation

- Added simple ophyd description; added file_manager description
  ([`48cfcb6`](https://github.com/bec-project/bec/commit/48cfcb6c6242c381aea71d0e1c686d10e3fb2c1b))

- Updated style; added css
  ([`6ec5fac`](https://github.com/bec-project/bec/commit/6ec5facd0cdf0588c6545828c53ccc9e8ed29875))

### Testing

- Fixed cached readout signature
  ([`74d63a1`](https://github.com/bec-project/bec/commit/74d63a19d2cbc3f84710227632171489f3ca5a93))

- Fixed client tests for new wait_for_server procedure
  ([`1934827`](https://github.com/bec-project/bec/commit/19348275eb0a7f9690621e53a4237a2db62b12bf))

- Fixed referenced readout
  ([`8ec7b23`](https://github.com/bec-project/bec/commit/8ec7b2329dda24548338d4c2d1bac39f4e0b208a))


## v0.16.2 (2023-08-05)

### Bug Fixes

- Fixed check_storage for already removed scan storage items
  ([`4a4dace`](https://github.com/bec-project/bec/commit/4a4daceaf4b7c579cb4adead784f9900b675b5dc))


## v0.16.1 (2023-08-05)

### Bug Fixes

- Added thread lock to file writer
  ([`27e85bb`](https://github.com/bec-project/bec/commit/27e85bb8b0e5afc0c70618438506727cea883253))


## v0.16.0 (2023-08-04)

### Bug Fixes

- Removed dummy link
  ([`de2c8ab`](https://github.com/bec-project/bec/commit/de2c8ab2c51357dd23e9efbf8481fa99adb11326))

- Removed unnecessary config assignment in client
  ([`9360570`](https://github.com/bec-project/bec/commit/93605707bd1ec1efea51407c593b25e0e5b75620))

### Build System

- Added option to install bec without redis / tmux
  ([`588968e`](https://github.com/bec-project/bec/commit/588968e2fa29f25d2f2ced59821e7d57ef3e1cf9))

### Documentation

- Added glossary
  ([`b54e56f`](https://github.com/bec-project/bec/commit/b54e56fe8fd7f29b2499770c7c392cdcf7e72fe8))

- Added logo
  ([`3c40a28`](https://github.com/bec-project/bec/commit/3c40a2856c7678d14517bfcae6fe2c935756f68d))

- Added missing reference file
  ([`df19570`](https://github.com/bec-project/bec/commit/df19570c9d658b35a04dbe7112c454793a8a2e54))

- Fixed indent
  ([`fe07a70`](https://github.com/bec-project/bec/commit/fe07a702df434714fd500fc983502e106e410bee))

- Fixed references
  ([`20254fb`](https://github.com/bec-project/bec/commit/20254fb628206f934238f40765a3fa5d15c3274c))

- Fixed requirements
  ([`76e9342`](https://github.com/bec-project/bec/commit/76e93429f6eb3851c5fabc78ff425e28b3ba2427))

- Redesigned documentation
  ([`ecf3ee9`](https://github.com/bec-project/bec/commit/ecf3ee93de1fd0ea0f4694150c8c07fcc21da4b5))

- Updated developer instructions
  ([`823094a`](https://github.com/bec-project/bec/commit/823094acb1b06074ef3180d2717986020b911b4f))

### Features

- Added done entry to filemessage
  ([`2c62fd7`](https://github.com/bec-project/bec/commit/2c62fd72b16cc62840daba929c1afd8dc26956d0))

- Added support for endpoints with and without suffix
  ([`ce0e54e`](https://github.com/bec-project/bec/commit/ce0e54e561ad5ef03898e749e7333dc7535bf0d2))

- Added support for file references and external links in the bec master file
  ([`9a59bdc`](https://github.com/bec-project/bec/commit/9a59bdce90110fded772bf4efd84b10e019a7837))

### Testing

- Fixed file writer test for new message endpoint
  ([`57e31b7`](https://github.com/bec-project/bec/commit/57e31b7da61fe555d2051bb0b8e38ad4752d3c9f))


## v0.15.0 (2023-08-03)

### Documentation

- Minor improvements for scan_to_csv docs
  ([`21d371a`](https://github.com/bec-project/bec/commit/21d371a80b8009e1df3c9d4148191f05a36a0abf))

- Updated sphinx conf file to deal with md files; added copy button
  ([`7f48ce6`](https://github.com/bec-project/bec/commit/7f48ce6aa1f2000993a4fb31e23a3efa3c122a57))

### Features

- Added option to specify config path as service config
  ([`1a776de`](https://github.com/bec-project/bec/commit/1a776de8118de7428b0c6b4e3693eaf619651192))


## v0.14.8 (2023-07-26)

### Bug Fixes

- Adapt write_to_csv to write multiple scan_reports for context manager
  ([`7118863`](https://github.com/bec-project/bec/commit/71188638323f27f0ae7f643a0e8b3ade12579899))

### Testing

- Update test case
  ([`9390ace`](https://github.com/bec-project/bec/commit/9390ace664692640df44b2ad1fb524338bf29747))


## v0.14.7 (2023-07-25)


## v0.14.6 (2023-07-25)

### Bug Fixes

- Fixed bec_client install
  ([`bacda25`](https://github.com/bec-project/bec/commit/bacda2580a47773bc4bdabc231049fb6470e7445))

- Fixed build
  ([`4eccc99`](https://github.com/bec-project/bec/commit/4eccc996694d9b260d1df40cc5b2c74ccb587dbe))


## v0.14.5 (2023-07-24)

### Bug Fixes

- Fixed install
  ([`3f42f2f`](https://github.com/bec-project/bec/commit/3f42f2f3e1d35e9d6f825a8f9865ab3dabf61be2))

### Refactoring

- Fixed formatter
  ([`b28b19f`](https://github.com/bec-project/bec/commit/b28b19f191c922752498c06a7f6d97a9618b3359))


## v0.14.4 (2023-07-24)

### Bug Fixes

- Added missing init files
  ([`1ea9764`](https://github.com/bec-project/bec/commit/1ea976411d320959a7826e6f09301df90b56517a))

- Added missing init files
  ([`29cf132`](https://github.com/bec-project/bec/commit/29cf132a06ebcec7f1e1a8f084d35da0195d4489))


## v0.14.3 (2023-07-24)

### Bug Fixes

- Fixed bec-server version
  ([`72fdd91`](https://github.com/bec-project/bec/commit/72fdd91da495e2150463c8aa64cab1a86577289e))

- Fixed build for device_server
  ([`fc90bfb`](https://github.com/bec-project/bec/commit/fc90bfb9aab5ef42a9c6160be71357f0df5d21bc))


## v0.14.2 (2023-07-24)

### Bug Fixes

- Fixed version update for bec-server
  ([`ae4673f`](https://github.com/bec-project/bec/commit/ae4673fac049e7bff799efb7566ea5a8fba56c57))


## v0.14.1 (2023-07-24)

### Bug Fixes

- Update version number directly to fix pip install without -e
  ([`91ffa4b`](https://github.com/bec-project/bec/commit/91ffa4b3c554ab4f0f038958344b81202e251433))


## v0.14.0 (2023-07-21)

### Bug Fixes

- Code update
  ([`86b1985`](https://github.com/bec-project/bec/commit/86b198595db33e1af6b8d2a26151658118b2ebe3))

- Fix writer functions
  ([`fda9d07`](https://github.com/bec-project/bec/commit/fda9d07e65039e833f51192d4a66a48875c3be46))

### Continuous Integration

- Enforce sphinx version
  ([`84d0a69`](https://github.com/bec-project/bec/commit/84d0a6923b19f634a475cb87c94d91b61190ad13))

- Removed docs build
  ([`6d37777`](https://github.com/bec-project/bec/commit/6d377772039ce9da269b35f18e650dee1ef6a0a5))

### Documentation

- Updated build dependencies
  ([`8dd2116`](https://github.com/bec-project/bec/commit/8dd21165f2079c64ac4e738d0f84926fd60cf887))

### Features

- Add new functions to save scan to dict and csv
  ([`effb642`](https://github.com/bec-project/bec/commit/effb642a4d3a099dd05e0f3b96ac727564e01999))

### Testing

- Add first tests
  ([`2899c4b`](https://github.com/bec-project/bec/commit/2899c4ba1b768b5b7993125bfb8917d5759f1d33))


## v0.13.3 (2023-07-21)

### Bug Fixes

- Fixed bec_server install
  ([`2ebf580`](https://github.com/bec-project/bec/commit/2ebf580ede20c594951bde73f2a570b744904509))

- Fixed single env install
  ([`929689c`](https://github.com/bec-project/bec/commit/929689cb8e7d1fccda0ab2a5a6372e2d48696193))

- Fixed tmux launch
  ([`e4d7840`](https://github.com/bec-project/bec/commit/e4d78402c0f0feca7d0731498b3b34701d9bc9a6))

### Build System

- Added black and pylint as dev dependencies
  ([`3f02a27`](https://github.com/bec-project/bec/commit/3f02a27ec47a5f1abd859d5f21c28b1da4c33400))

### Refactoring

- Cleanup
  ([`bc58541`](https://github.com/bec-project/bec/commit/bc585414021fc18b7dc848ff22a5ce6e1672201a))

- Cleanup
  ([`424e7a1`](https://github.com/bec-project/bec/commit/424e7a1db25ef1d1c99da5a3fd8914c1c27123c0))


## v0.13.2 (2023-07-21)

### Bug Fixes

- Pip install dev environment
  ([`750fe66`](https://github.com/bec-project/bec/commit/750fe66ed3c7c813b9ea154055f6a6f599fadc9a))

### Documentation

- Added missing glossary file
  ([`2529891`](https://github.com/bec-project/bec/commit/2529891a2ca39e773651c3b96d70584c55115eab))

- Fixed dependencies; added missing files
  ([`87e7ec2`](https://github.com/bec-project/bec/commit/87e7ec2671578ffb2f5c6db1f5d98fcdebaeb61f))

- Improved documentation; added how tos; added glossary
  ([`99f0c96`](https://github.com/bec-project/bec/commit/99f0c9636b36f89dc156959184cdd31d65ffee5c))

- Removed user api for now
  ([`d8fd1d0`](https://github.com/bec-project/bec/commit/d8fd1d0b984f4a32d090cfbebcf9a6511f734e09))


## v0.13.1 (2023-07-18)

### Bug Fixes

- Fixed bug in BECMessage str dunder
  ([`65e76a9`](https://github.com/bec-project/bec/commit/65e76a93ceec953434e23432b9c5e912eabcb2c0))

### Continuous Integration

- Fixed python-semantic-release version to 7.*
  ([`3b203f8`](https://github.com/bec-project/bec/commit/3b203f873dac520e2a26540107b4474931b31cfe))


## v0.13.0 (2023-07-14)

### Continuous Integration

- Added secret detection
  ([`ab9a833`](https://github.com/bec-project/bec/commit/ab9a833a2e60715781ef62d78a5ddd264cd89fc2))

- Renamed unittest stage to test
  ([`e990a44`](https://github.com/bec-project/bec/commit/e990a44f7df88fec8c47089dc8d15065cf273d1c))

### Features

- Triggering release after refactoring (file_writer_mixin)
  ([`e4a51b6`](https://github.com/bec-project/bec/commit/e4a51b67a63bdde93c91e07e7428759c4eb44d56))

### Refactoring

- Moved file writer functions to bec_lib
  ([`742dfff`](https://github.com/bec-project/bec/commit/742dfff8ba40cb99d4451a4d10788c1e563a1b7c))


## v0.12.0 (2023-07-12)

### Bug Fixes

- Improvements / fixes for redis streams
  ([`3f09cc3`](https://github.com/bec-project/bec/commit/3f09cc3cd153e629ee550072d7fc5c31100594be))

### Features

- Added message version 1.2 for better performance
  ([`f46b29a`](https://github.com/bec-project/bec/commit/f46b29a2427137be86903df7da6684613698d0c7))

### Testing

- Improved tests for redis connector
  ([`5d892fe`](https://github.com/bec-project/bec/commit/5d892fe34550883b2a9714b6c8ee7d243e8ecfd5))


## v0.11.0 (2023-07-12)

### Bug Fixes

- Fixed bundled messages for 1.2
  ([`9381c7d`](https://github.com/bec-project/bec/commit/9381c7d64684c332b90480aa8c7a6774baf3b5dd))

- Improvements / fixes for redis streams
  ([`72e4f94`](https://github.com/bec-project/bec/commit/72e4f943b684e53e16ed11538d0807d012e9e357))

### Features

- Added message version 1.2 for better performance
  ([`fe2bd6c`](https://github.com/bec-project/bec/commit/fe2bd6c935b511d26a649f89f4ba5b44ed01b7f0))

- Added redis stream methods to RedisProducer
  ([`e8352aa`](https://github.com/bec-project/bec/commit/e8352aa606dc999f0e1bf1bd891a7852a489509d))


## v0.10.2 (2023-07-11)

### Bug Fixes

- Added missing x coords to lmfit processor
  ([`ddfe9df`](https://github.com/bec-project/bec/commit/ddfe9df6a11f506e52f00be59f76b43c910d0504))


## v0.10.1 (2023-07-11)

### Bug Fixes

- Fixed relative path in client init; needed for pypi
  ([`0d9ed33`](https://github.com/bec-project/bec/commit/0d9ed33a2d63e54ac12bf9cd5dcc6d4250e70bc4))


## v0.10.0 (2023-07-08)

### Bug Fixes

- Added missing services to the build script
  ([`6d45485`](https://github.com/bec-project/bec/commit/6d45485b5a83d02612595c25a3fd3ec90f0c57b6))

- Adjusted import routine for plugins
  ([`38c4c8c`](https://github.com/bec-project/bec/commit/38c4c8c93e79a37314ad5579feb77455d2a5e38f))

- Fixed bug in install script
  ([`1a7a4d8`](https://github.com/bec-project/bec/commit/1a7a4d8a745ea29af4ccdc03b6b4d608b6b18fa8))

- Fixed bug in install script
  ([`05bf99a`](https://github.com/bec-project/bec/commit/05bf99af739b4023ad75780fe2808f71adcc508f))

- Fixed bug in ipython live update
  ([`a6a2c28`](https://github.com/bec-project/bec/commit/a6a2c28a6a111ff552277686d7455eec9cbd56d1))

- Fixed missing files
  ([`047082b`](https://github.com/bec-project/bec/commit/047082b38b7f4145c469a76f439fcac241a92b60))

- Improved tmux_launcher to handle merged and separated envs
  ([`088b1a4`](https://github.com/bec-project/bec/commit/088b1a4a1956209c11c5a31f5c09eca8aed6b86a))

### Build System

- Fixed bec-server config parser
  ([`85ecf30`](https://github.com/bec-project/bec/commit/85ecf3083a304e8f505188a11486657b33b29f86))

### Continuous Integration

- Added en2end test
  ([`103bc9d`](https://github.com/bec-project/bec/commit/103bc9d0e022ea2b4ae9ed26914a07e5c573cab5))

- Added en2end test
  ([`1980e87`](https://github.com/bec-project/bec/commit/1980e871c119cc4e16d8564d1d51757de9f483d0))

- Added en2end test
  ([`4376a98`](https://github.com/bec-project/bec/commit/4376a9876d78bae182252b5051e020c4afd7e286))

- Added en2end test
  ([`7c57bc5`](https://github.com/bec-project/bec/commit/7c57bc5a33cc2d0dc9406c564d16d91d8278faa2))

- Added end2end test with conda
  ([`779507b`](https://github.com/bec-project/bec/commit/779507ba61e2a552422eb9b2f5800964b3c8270b))

- Added end2end test with conda
  ([`a131196`](https://github.com/bec-project/bec/commit/a1311964be6751c630ae227d8b4d06600c1afa06))

- Added end2end test with conda
  ([`69092ff`](https://github.com/bec-project/bec/commit/69092ff6f8833a10dd5485e60690315c8fa59f20))

- Added end2end test with conda
  ([`9f49a16`](https://github.com/bec-project/bec/commit/9f49a166631c0ea2bafa92960c063c980bec1539))

- Added end2end test with conda
  ([`c7307ed`](https://github.com/bec-project/bec/commit/c7307ed93a240c12c93455c31f67e06a8e791781))

- Added end2end test with conda
  ([`acf4e11`](https://github.com/bec-project/bec/commit/acf4e11544498fd16e138e303ab1446ea3bd46e7))

- Added tmux
  ([`f7c9f49`](https://github.com/bec-project/bec/commit/f7c9f49c875a2d2fdc9d1bd448df54460b33c30e))

- Added tmux
  ([`a709030`](https://github.com/bec-project/bec/commit/a709030b5c5da412cd80176debdeef3b9526931c))

- Added tmux
  ([`1212ef4`](https://github.com/bec-project/bec/commit/1212ef41226f238a4be98b25624ad8f0d41f74ad))

- Changed init order
  ([`331d282`](https://github.com/bec-project/bec/commit/331d282a0b8a73452b06da53e85fca95ccc9fac6))

- Fixed ci path
  ([`96c44f6`](https://github.com/bec-project/bec/commit/96c44f6c5f31537a41a4c055bfd827ae586cc833))

### Documentation

- Updated deployment instructions
  ([`390db04`](https://github.com/bec-project/bec/commit/390db0442266f1d4fc36bf8beb70715ccb692eea))

- Updated documentation for new deployment
  ([`dfc8c92`](https://github.com/bec-project/bec/commit/dfc8c9247d6b4891cdfb489be2bd3dfba5fe8f40))

### Features

- Added clis to all services; added bec_server
  ([`f563800`](https://github.com/bec-project/bec/commit/f563800268e7047fd9baa05e48070475688b244f))

- Added default service config
  ([`b1a4b4f`](https://github.com/bec-project/bec/commit/b1a4b4f75cad19e849d573beb767b18c6d93a308))

- Added install_bec_dev script
  ([`db9539a`](https://github.com/bec-project/bec/commit/db9539aba203e7e299620f76dfd1f3843ebfecbd))

- Simplified bec-server interaction; removed hard-coded service config path
  ([`5dd1eb7`](https://github.com/bec-project/bec/commit/5dd1eb7cd0ea0d401c411c9e46b8a567e58c9687))

### Refactoring

- Refactored tmux launch
  ([`72bdf58`](https://github.com/bec-project/bec/commit/72bdf5824b6ebd7f4aadd5e5d6f094bbe5f31331))

- Unified optional dependency messages
  ([`35c4c6a`](https://github.com/bec-project/bec/commit/35c4c6aecd7e8478c1f62eab129db19bb0d3c4bf))

### Testing

- Added more tests
  ([`441afec`](https://github.com/bec-project/bec/commit/441afec08bc4e244ca86524eac724e9ee6616721))

- Fixed formatting
  ([`9d1b1c9`](https://github.com/bec-project/bec/commit/9d1b1c9d3ae1a324ea9134ce8652963f82375009))

- Fixed tests
  ([`aae5e1a`](https://github.com/bec-project/bec/commit/aae5e1a05033709f8caccce34372f4caed13da42))

- Fixed threading problem in callback test
  ([`6d56d90`](https://github.com/bec-project/bec/commit/6d56d901b137070e65d51d140f2035128a4b4f7a))


## v0.9.2 (2023-07-04)

### Bug Fixes

- Added reset_device function
  ([`f235a17`](https://github.com/bec-project/bec/commit/f235a1735f67f25eab9ae4ed746a1c101da43dc9))

- Fixed bug in client callbacks that caused rejected scans to get stuck
  ([`2611f5b`](https://github.com/bec-project/bec/commit/2611f5b4232fed7d930b21059c2cd0e8a1098a3a))

- Fixed bug in ipython_live_updates in case of missing status messages
  ([`39c4323`](https://github.com/bec-project/bec/commit/39c4323303287617918d7cd7101332b338026954))

- Fixed re-enabling devices
  ([`3f11144`](https://github.com/bec-project/bec/commit/3f111442584b9abf39382620ccf137c93c89d6a8))

- Improved getattr handling for dunder methods; added comment
  ([`a6c49b3`](https://github.com/bec-project/bec/commit/a6c49b34ad2a6960c9db57b6ab6336bb94b432d9))

### Build System

- Added dev option
  ([`e669fb9`](https://github.com/bec-project/bec/commit/e669fb918804517dc3969ed0dc95977de69c2a90))

### Refactoring

- Removed unnecessary return
  ([`ba1f856`](https://github.com/bec-project/bec/commit/ba1f8563ae74dbe9d5f09a67b8ad87f1d294e699))

### Testing

- Added config handler tests
  ([`734c6d0`](https://github.com/bec-project/bec/commit/734c6d0a36eafcef4c4c3398b3f7600b10c8ee5f))

- Added ipython client tests
  ([`a927938`](https://github.com/bec-project/bec/commit/a927938c53856331fe3107ccc5767543d1dc64b7))

- Fixed client test
  ([`396f60f`](https://github.com/bec-project/bec/commit/396f60f687f52a255cb7171dd6bd3369d939daac))

- Fixed client test
  ([`e0330df`](https://github.com/bec-project/bec/commit/e0330df80c21d2c2c9d502b7fe3406726f6a479b))

- Fixed test
  ([`0f1161b`](https://github.com/bec-project/bec/commit/0f1161bd828d77b853373ddad9510218e9c48646))

- Improved client tests
  ([`a857cff`](https://github.com/bec-project/bec/commit/a857cffa7907698ed9a64fffb7cda368201d1a77))


## v0.9.1 (2023-07-03)

### Bug Fixes

- Fixed bug in device_manager that killed tab-completion
  ([`32d313a`](https://github.com/bec-project/bec/commit/32d313a04feee1437b4aff547b3ba998266d78af))


## v0.9.0 (2023-07-02)


## v0.8.1 (2023-07-02)

### Bug Fixes

- Fixed ipython client startup script for new lib name
  ([`b2f5f3c`](https://github.com/bec-project/bec/commit/b2f5f3c2631d749ade619fd32b3f10671f9f3f1c))

### Continuous Integration

- Updated docker-compose.yaml
  ([`3f7a41f`](https://github.com/bec-project/bec/commit/3f7a41fd971a4cd2b83e0b5603bded7755915f9b))

### Documentation

- Added data_processing services; changed default python version to 3.9
  ([`233f682`](https://github.com/bec-project/bec/commit/233f68216ff12ce223ea4024fe190e237df21afe))

- Updated doc with proper semver
  ([`71aa1d7`](https://github.com/bec-project/bec/commit/71aa1d715a47a9b42888147611ffc8af8d46714c))

### Features

- Add support for scan plugins set through environment vars
  ([`5ad0d9b`](https://github.com/bec-project/bec/commit/5ad0d9bbe49c5a0aa1bed74f19caf8df553ee98e))


## v0.8.0 (2023-06-28)

### Features

- Renamed primary devices to monitored devices; closes #75
  ([`1370db4`](https://github.com/bec-project/bec/commit/1370db4c70b08702c29e3728b8d0c3229d0188f3))


## v0.7.1 (2023-06-28)

### Bug Fixes

- Remove outdated requirements.txt files
  ([`f781571`](https://github.com/bec-project/bec/commit/f7815714ff9c9ab6c5b697edc651c376c8052e70))

- Setup files cleanup
  ([`f60889a`](https://github.com/bec-project/bec/commit/f60889a87e16ff767806d47bd82a988f50fb091d))

### Continuous Integration

- Reverted to ophyd_devices master branch
  ([`79d5881`](https://github.com/bec-project/bec/commit/79d58812cfc618b631dd265942a1bbc2c47052e4))


## v0.7.0 (2023-06-28)

### Continuous Integration

- Fixed ophyd_devices_branch
  ([`3974cd4`](https://github.com/bec-project/bec/commit/3974cd49a640d959983de560862b93cb9046c23f))

### Documentation

- Updated readme for new bec_lib
  ([`6e0bf12`](https://github.com/bec-project/bec/commit/6e0bf12a0ae1885245961461f0bcef09ad13c2ec))

### Features

- Renamed bec_client_lib to bec_lib
  ([`a944e43`](https://github.com/bec-project/bec/commit/a944e43e1a8db55959a042a8203040fa2c5484ba))


## v0.6.14 (2023-06-27)

### Bug Fixes

- Testing build
  ([`6849b95`](https://github.com/bec-project/bec/commit/6849b9583ff0b3c5f4b49180f78b1ef612669145))

### Continuous Integration

- Added job stage dependency to deploy stage
  ([`15ebb27`](https://github.com/bec-project/bec/commit/15ebb275975f862ab15724c623950917ec53858e))

- Changes build order
  ([`c7afe0f`](https://github.com/bec-project/bec/commit/c7afe0f15e23d2047bae5d207f6ccc6effef5a21))

### Documentation

- Added readme for bec-client-lib
  ([`bd39147`](https://github.com/bec-project/bec/commit/bd391470f86ece1e26b629c75341b2ee2c941da4))

- Added scan server readme
  ([`1663087`](https://github.com/bec-project/bec/commit/1663087ff8866dff31a6974474b56dd3e73ffb1d))


## v0.6.13 (2023-06-27)

### Bug Fixes

- Added env vars
  ([`3d33d4b`](https://github.com/bec-project/bec/commit/3d33d4bc32d2daac29cef6e71d5e0d48aba54f7e))


## v0.6.12 (2023-06-27)

### Bug Fixes

- Build test
  ([`899cfab`](https://github.com/bec-project/bec/commit/899cfaba35fe40457635d3c8b9840da762e4b0ba))


## v0.6.11 (2023-06-27)

### Bug Fixes

- Build process with env var
  ([`3c5f351`](https://github.com/bec-project/bec/commit/3c5f35166af19faa51fef75aed48a3ded0a186e4))

- Testing build
  ([`5f20c5e`](https://github.com/bec-project/bec/commit/5f20c5e32d304e973bf02e496b1c0bcc6990a302))


## v0.6.10 (2023-06-27)


## v0.6.9 (2023-06-27)

### Bug Fixes

- Testing build
  ([`d5fb551`](https://github.com/bec-project/bec/commit/d5fb5511c79dfc598089d42a184fca26a35e6b3b))


## v0.6.8 (2023-06-27)

### Bug Fixes

- Fixed build script
  ([`5bba42a`](https://github.com/bec-project/bec/commit/5bba42a898c2b8ec5735d1f059012ac60e2222a9))

- Testing release
  ([`240d402`](https://github.com/bec-project/bec/commit/240d4020b80f371d3001a59fe55ac1433edb93d9))

### Continuous Integration

- Added external shell script for build process
  ([`1d801c7`](https://github.com/bec-project/bec/commit/1d801c7feb28a5e286a605aaa78034c0a6f5b785))


## v0.6.7 (2023-06-27)

### Bug Fixes

- Fixed and improved setup.cfg files
  ([`b04a97e`](https://github.com/bec-project/bec/commit/b04a97edbb4309d0364f19df528401ad29c62c9b))

### Continuous Integration

- Added pypi builds
  ([`0579200`](https://github.com/bec-project/bec/commit/0579200acb6898e9fec881e11d57c12f84b54db9))

- Fixed docker image; moved docker compose to private registry
  ([`3e9dc7f`](https://github.com/bec-project/bec/commit/3e9dc7f459ca13a7390c201ec33132a4262973a1))

- Fixed missing pytest dependency
  ([`4ccc66a`](https://github.com/bec-project/bec/commit/4ccc66a3a73c28d4a2312bfa0e850f3eb02d6f17))

- Moved to morgana-harbor registry
  ([`de370db`](https://github.com/bec-project/bec/commit/de370db0376150293d9f1209a8923038e618d67b))

- Removed static mongodb version
  ([`55f0716`](https://github.com/bec-project/bec/commit/55f071631df53932863f934369bb14c67b592bab))

- Set testpypi as target repository
  ([`d0d8aff`](https://github.com/bec-project/bec/commit/d0d8afff556ebe151949d860e012ee899ac0c956))

### Documentation

- Improved config helper doc strings
  ([`08f6ff4`](https://github.com/bec-project/bec/commit/08f6ff444e24395ee759f203123d9962441f59dd))

### Testing

- Added config_helper tests
  ([`dd10525`](https://github.com/bec-project/bec/commit/dd10525df4507c87c9d040412f62807b2fcd3f0b))


## v0.6.6 (2023-06-25)

### Bug Fixes

- Fixed file writer for empty time stamps
  ([`bc5fbf6`](https://github.com/bec-project/bec/commit/bc5fbf651c39c562de2b2568011c47094e155017))


## v0.6.5 (2023-06-25)

### Bug Fixes

- Fixed timestamps for h5 files; closes #76
  ([`36ab89e`](https://github.com/bec-project/bec/commit/36ab89e51e031697f1611a1a1c5b946d3c7c1c2a))

### Continuous Integration

- Fix async callback test
  ([`08cfdff`](https://github.com/bec-project/bec/commit/08cfdffb5a2c85e4d5f6fcbb33f01d7e1b9573ea))

### Refactoring

- Minor refactoring
  ([`1800e78`](https://github.com/bec-project/bec/commit/1800e788c08cdd00505f1084735762d967ed25e8))


## v0.6.4 (2023-06-23)

### Bug Fixes

- Added missing remove_device_tag function
  ([`a0884ce`](https://github.com/bec-project/bec/commit/a0884cea22ee32026753b0cec449c7003a2b49b5))

### Testing

- Added more device_manager tests
  ([`08e60a0`](https://github.com/bec-project/bec/commit/08e60a0513cedcbc41d61272932b2e6cd412cad1))


## v0.6.3 (2023-06-23)

### Bug Fixes

- Fixed typo
  ([`3cc4418`](https://github.com/bec-project/bec/commit/3cc44186ab8ada514c7d950bd2acbb5b03ac8e25))

- Version variable is pulled from semantic release file
  ([`6669bce`](https://github.com/bec-project/bec/commit/6669bce3e178ca71d664adf9a7493e7ecad4589d))

### Continuous Integration

- Fixed docker setup
  ([`89c3b96`](https://github.com/bec-project/bec/commit/89c3b96b5c651657c613df9cc12b8ed43d968553))


## v0.6.2 (2023-06-23)

### Bug Fixes

- Fixed scan item for intermediate repr queries
  ([`a915a69`](https://github.com/bec-project/bec/commit/a915a6906667cff85ab62e22a9bb0ec8f96a2656))

### Refactoring

- Use scan item repr in scan report
  ([`85d0c44`](https://github.com/bec-project/bec/commit/85d0c44cff1d478c784dfa9226d7f7d2d34a956f))

### Testing

- Added scan item tests
  ([`8e63cfc`](https://github.com/bec-project/bec/commit/8e63cfcc9b587f51fab5dc5418d55dc362832234))


## v0.6.1 (2023-06-23)

### Bug Fixes

- Fixed monitor scan for numpy v1.25
  ([`870c033`](https://github.com/bec-project/bec/commit/870c03344cd55d22a89d236d88ec60e7677ed20e))

- Fixed scan item for intermediate repr queries
  ([`9decff2`](https://github.com/bec-project/bec/commit/9decff27a74af7d84f41ddd8f9b3585e1d353a88))

### Continuous Integration

- Fixed branch reset
  ([`6eeb8b0`](https://github.com/bec-project/bec/commit/6eeb8b087421c0f1d2d4ba8fb9cdfe92e147d3ff))

### Documentation

- Improved doc strings for scans
  ([`25fe364`](https://github.com/bec-project/bec/commit/25fe3641442f1fe31000685664881aaa01c9cfb3))

### Refactoring

- Minor cleanup
  ([`5e40221`](https://github.com/bec-project/bec/commit/5e4022161774f3b6125176041877852d7163fafd))

- Use scan item repr in scan report
  ([`33dda01`](https://github.com/bec-project/bec/commit/33dda012ccdde515fdbf2f6811895a0f59c9e376))

### Testing

- Added scan item tests
  ([`2eff23b`](https://github.com/bec-project/bec/commit/2eff23bd7d59e89ebb948a3cc6da21e3ce1cd73d))

- Added scan server tests
  ([`4f0b9b0`](https://github.com/bec-project/bec/commit/4f0b9b046920470f073bcb5be1e181fa12eb3a74))

- Fixed scan item test
  ([`1b27b54`](https://github.com/bec-project/bec/commit/1b27b54fa8338d3c05984645599c99115025b689))

- More scan server tests
  ([`00ad593`](https://github.com/bec-project/bec/commit/00ad593d5cdb56d876a8ca3148c8e3dc0c9261dd))


## v0.6.0 (2023-06-22)

### Continuous Integration

- Fixed switch to branch/tag
  ([`251d7d5`](https://github.com/bec-project/bec/commit/251d7d57786afcea1e8c8d0fed39ae3fb91087dc))

### Features

- Add to_pandas method to scan items
  ([`858bb78`](https://github.com/bec-project/bec/commit/858bb7816d02e0326492cc6d53a18d3b4fa646e9))


## v0.5.0 (2023-06-20)

### Continuous Integration

- Removed workflow for now
  ([`191c92e`](https://github.com/bec-project/bec/commit/191c92eed3cc51e7a0b83eda8f78c06e4ac9dc7d))

### Documentation

- Add commit message info to readme
  ([`2d8038b`](https://github.com/bec-project/bec/commit/2d8038bac7ecb3563025ceddfe08b177f94bdf6c))

### Features

- Added bec data processing service
  ([`17213da`](https://github.com/bec-project/bec/commit/17213da46b236cb5ff7155890e4319308350ba4c))

- Added dap message and endpoint
  ([`e1aa5e1`](https://github.com/bec-project/bec/commit/e1aa5e199b10cf9c7570967c01a5f3b48bfe1fc6))


## v0.4.9 (2023-06-19)

### Bug Fixes

- Raise when device does not exist; added str dunder for devices
  ([`12e2d29`](https://github.com/bec-project/bec/commit/12e2d29dad71c11586cee06cb0688557c3cb4bb2))

### Documentation

- Added more doc strings
  ([`c8cc156`](https://github.com/bec-project/bec/commit/c8cc15632d4221877a19296bb7d8b7742c1e4ccd))

### Testing

- Fixed device init
  ([`6339246`](https://github.com/bec-project/bec/commit/6339246cb18e0f1b15f435fdcce06a7d264a17a5))

- Fixed tests for unknown devices
  ([`4d10165`](https://github.com/bec-project/bec/commit/4d10165a81c7d87076544e335b2ce8c8a483cd02))


## v0.4.8 (2023-06-19)

### Bug Fixes

- Removed changelog dependency
  ([`2be1c67`](https://github.com/bec-project/bec/commit/2be1c67cbc7a025314a665c6d272ac2874e02fee))

### Continuous Integration

- Added default run
  ([`b04e38c`](https://github.com/bec-project/bec/commit/b04e38cef5128e0c2f08e88125c4dfaa7055fede))

- Added workflow
  ([`4383157`](https://github.com/bec-project/bec/commit/438315712643a879e5a883d69a8c94de0a05f8f4))


## v0.4.7 (2023-06-19)

### Bug Fixes

- Fixed typo
  ([`f59e73c`](https://github.com/bec-project/bec/commit/f59e73cbb11c1242115b1b42b97cbeb0f0f6252b))

- Fixed weird semantic-release syntax
  ([`eabb210`](https://github.com/bec-project/bec/commit/eabb210b6e0e68269854026a8a71c07cd9274c04))


## v0.4.6 (2023-06-19)

### Bug Fixes

- Removed pypi upload
  ([`0b28025`](https://github.com/bec-project/bec/commit/0b280253701b5e49ea37512cda6bad888e4b8149))


## v0.4.5 (2023-06-19)

### Bug Fixes

- Removed build
  ([`1171e65`](https://github.com/bec-project/bec/commit/1171e651959df0b07f4e9ce096a8b6e4e77b132b))


## v0.4.4 (2023-06-19)

### Bug Fixes

- Disabled upload to repository
  ([`2e56468`](https://github.com/bec-project/bec/commit/2e564681016b1b369c65087ab447444eca8a2c9a))


## v0.4.3 (2023-06-19)

### Bug Fixes

- Added git pull
  ([`7e77444`](https://github.com/bec-project/bec/commit/7e77444a70647706ae448186fb44c64a3622880c))

- Added hvcs domain
  ([`32856c5`](https://github.com/bec-project/bec/commit/32856c50047c6a91a10f2a3666738dc6b7f16737))

- Checkout master instead of commit
  ([`33e0669`](https://github.com/bec-project/bec/commit/33e0669323e3fb01d079fb56018349d190537101))

- Fixed domain name
  ([`a3c2e5f`](https://github.com/bec-project/bec/commit/a3c2e5ff85dbdd6badc182828ee85b6e01dc6377))

- Pull from origin
  ([`6c659a9`](https://github.com/bec-project/bec/commit/6c659a94c4dbd5b7a4a3718c08b6fd1b117c3602))


## v0.4.2 (2023-06-19)

### Bug Fixes

- Changed semantic-release version to publish
  ([`5e12ef4`](https://github.com/bec-project/bec/commit/5e12ef43171b6b75abd666aabd9060f132e53fce))

- Delete all local tags before adding new ones
  ([`b8d71f5`](https://github.com/bec-project/bec/commit/b8d71f5cabf80d099bf76687758f398bff9214e1))

- Np.vstack must receive tuple
  ([`3286d46`](https://github.com/bec-project/bec/commit/3286d46163e4ce7d262c170a0d04a59f287b40c1))

- Only run semver on master; added git tag log
  ([`b63d128`](https://github.com/bec-project/bec/commit/b63d128cefb50dfdb52328bae7032a22cd9d5934))


## v0.4.1 (2023-06-19)


## v0.4.0 (2023-06-16)

### Bug Fixes

- Added bec status updates
  ([`01bf0a3`](https://github.com/bec-project/bec/commit/01bf0a35dbe28928d634ce6c7e71e5948d2c53c0))

- Added bpm4i to default csaxs config
  ([`b04cf72`](https://github.com/bec-project/bec/commit/b04cf72e5303483658cbae3d71378aa864d42233))

- Added check_alarms to wait function
  ([`d5abeb8`](https://github.com/bec-project/bec/commit/d5abeb89fa9d259310bc048d9de5829d798af34d))

- Added config converter and validator in the absence of scibec
  ([`f0a7c9f`](https://github.com/bec-project/bec/commit/f0a7c9faf2affbbcfdb4506e41e02866dcaa22e0))

- Added dummy fshon
  ([`2ef2d59`](https://github.com/bec-project/bec/commit/2ef2d599199c3d1294113476ad08f78e32e8fd3f))

- Added error handling to catch failed device inits
  ([`8036c4b`](https://github.com/bec-project/bec/commit/8036c4b26e8eda082cd0bfb541604d7e787dc676))

- Added fpdf dependency
  ([`d317dd8`](https://github.com/bec-project/bec/commit/d317dd8229db6d1fc3cf1e65adc0e7f4bd34afaa))

- Added jsonschema to dependencies
  ([`a4bab2b`](https://github.com/bec-project/bec/commit/a4bab2b715adfd4d1ba84fd0e23c5a7bedc9b2da))

- Added missing file
  ([`2cb3a5b`](https://github.com/bec-project/bec/commit/2cb3a5b00f849ea52a234bd4a78550da878a076e))

- Added missing file
  ([`f256ec1`](https://github.com/bec-project/bec/commit/f256ec1839ce1fdaea434452d253e998836a9736))

- Added missing files
  ([`97b374f`](https://github.com/bec-project/bec/commit/97b374f024fdafb4a19a16a3a360c56a6007f071))

- Added missing files
  ([`baf2367`](https://github.com/bec-project/bec/commit/baf23679afd4011b45bde8277dfb0d8cb085a670))

- Added missing files
  ([`82953f9`](https://github.com/bec-project/bec/commit/82953f9d4edf5638c1e2bbe8d1f69b8cc3bc1a74))

- Added missing websocket file
  ([`6669a39`](https://github.com/bec-project/bec/commit/6669a396cac76921dbdcaf5bfd4441a212ad6b0b))

- Added redis config update to scihub init
  ([`821f940`](https://github.com/bec-project/bec/commit/821f940470bd39b5bd6264bd02331db1d0263270))

- Added scan status messages for halted and paused
  ([`c239c41`](https://github.com/bec-project/bec/commit/c239c41394e47ea9abb8846fa18d483abbb8e8a5))

- Added scihub dependency
  ([`791fea1`](https://github.com/bec-project/bec/commit/791fea1735e6fb02e627cb418cf6f0ce6f30fba7))

- Added semver log to pre-commit
  ([`aeb5fcb`](https://github.com/bec-project/bec/commit/aeb5fcbed63037047d61bbe30a9a00996fe31d4c))

- Added semver to all services
  ([`b32edb5`](https://github.com/bec-project/bec/commit/b32edb549067334c8dfc68f060c112e8ba7230ad))

- Added sleep to ensure that the device server starts processing the data first. Should be changed
  to a reply from SciHub
  ([`e65a6a2`](https://github.com/bec-project/bec/commit/e65a6a23cbab9bbd231459f0953392150b6fc7d4))

- Added threadlock to scan queue status
  ([`5912c0f`](https://github.com/bec-project/bec/commit/5912c0f02b9ea44e8759c53a4b760e6d1b0bc910))

- Added username to semver deploy
  ([`1c3fc4d`](https://github.com/bec-project/bec/commit/1c3fc4d01ed00120229559ed6a86fb2b1907d937))

- Added wait for device server before loading the session
  ([`1cff100`](https://github.com/bec-project/bec/commit/1cff1009aa5e51f6788a2b50bce77fb0f87d1ea7))

- Bug fixes related to client_lib refactoring
  ([`79d5604`](https://github.com/bec-project/bec/commit/79d5604e67443c161b56d7164774f81d596d86d5))

- Bug fixes related to client_lib refactoring
  ([`0a58846`](https://github.com/bec-project/bec/commit/0a588462f547707222c625676782ce77060e858f))

- Cb must be specified
  ([`d79d005`](https://github.com/bec-project/bec/commit/d79d005527f54a2a8f43dbd87a31c8544f73f4c5))

- Change message to logger for _trigger_device
  ([`5d662d1`](https://github.com/bec-project/bec/commit/5d662d17ad099095350aea7ce2b6b878226d0847))

- Change so that the topics creates a list only when topics is not None
  ([`e31fe6d`](https://github.com/bec-project/bec/commit/e31fe6d8b729ac31512be93add108202efb23f0c))

- Changed so that all fcns does the same
  ([`733ebc7`](https://github.com/bec-project/bec/commit/733ebc70361d52e8991cab96020b85598c73934e))

- Changes related to monitor scan
  ([`d2dbc2f`](https://github.com/bec-project/bec/commit/d2dbc2fb28ac11e05ac5f1696fe4203597622f0e))

- Changes related to readout_priority
  ([`a65aa9d`](https://github.com/bec-project/bec/commit/a65aa9d055d0b09aa02f2be40cb9377c9f1ccbd7))

- Checkout correct ophyd branch
  ([`1ec4567`](https://github.com/bec-project/bec/commit/1ec4567918e2152d90e30080e619e2fe6975e389))

- Cleanup after lamni
  ([`ac2fbc1`](https://github.com/bec-project/bec/commit/ac2fbc18db9eca42b5536044561491e633e7de93))

- Disabled quadems for now as they fail to connect
  ([`0fc45ae`](https://github.com/bec-project/bec/commit/0fc45ae58316077241c4619b504aa54c45c438d1))

- Fixed a typo
  ([`a314210`](https://github.com/bec-project/bec/commit/a314210a01b976c930713cd4d6d7f844fc8be528))

- Fixed abort level for alarms
  ([`548d55e`](https://github.com/bec-project/bec/commit/548d55ea90aeccedcf7099a0c2e7ea43d4a037dd))

- Fixed alarm_handler verbosity; changed default to Minor
  ([`8e3f389`](https://github.com/bec-project/bec/commit/8e3f3891ce77cf69f929becea1b5357fe5d6b412))

- Fixed async callbacks for BECClient
  ([`087595d`](https://github.com/bec-project/bec/commit/087595dbf45a29818e6a53e631a9d7d5e4a71fa7))

- Fixed async callbacks for BECClient
  ([`337d6e6`](https://github.com/bec-project/bec/commit/337d6e61723d6cbf1db7b096bd39bdab03b1d9b3))

- Fixed beamline mixin inheritance
  ([`897e5d1`](https://github.com/bec-project/bec/commit/897e5d1a38282efe7faa3adb7b803e3f998f1e43))

- Fixed bec magics for new live_updates structure
  ([`5433b35`](https://github.com/bec-project/bec/commit/5433b35d4a8c2230d9f506994eabb030a37d414b))

- Fixed bec messages for numpy data; fixed message reader
  ([`96a5f1b`](https://github.com/bec-project/bec/commit/96a5f1b49d2a322c7c1d591f48d3ffc6f1e70091))

- Fixed bec_utils dependency
  ([`f000f1c`](https://github.com/bec-project/bec/commit/f000f1c87a8694ffccdb0ba1e5ca7b945ea6486c))

- Fixed bl show output
  ([`58b1e25`](https://github.com/bec-project/bec/commit/58b1e250fcd0f25aba85c4809218c2471eb797a0))

- Fixed bpm4i config
  ([`ed143e0`](https://github.com/bec-project/bec/commit/ed143e0204b8d3461bc3228310fa67c92d25e19f))

- Fixed bug for movements without report instructions
  ([`4716a54`](https://github.com/bec-project/bec/commit/4716a54e290e8b092c883964934f0f78d360c522))

- Fixed bug for umv
  ([`3466e60`](https://github.com/bec-project/bec/commit/3466e608f258b0f1af59325ec9858edd2f0c6c52))

- Fixed bug for undefined status callbacks
  ([`a6fe9e6`](https://github.com/bec-project/bec/commit/a6fe9e614d9618be24c5da3d7604532fbfcb9fd4))

- Fixed bug in bec_client init
  ([`fff063c`](https://github.com/bec-project/bec/commit/fff063ce2f8becdd9431eb441c525e9c12b6b43b))

- Fixed bug in ci file
  ([`64fd8d0`](https://github.com/bec-project/bec/commit/64fd8d0696479fd03d9f898fe833e0754e03485b))

- Fixed bug in file_writer
  ([`3f21d40`](https://github.com/bec-project/bec/commit/3f21d406139ae0577ee5ef6752be448053ad9bc1))

- Fixed bug in init_config
  ([`6d33cbe`](https://github.com/bec-project/bec/commit/6d33cbe6bb65d5e36dab7857f3359fd5dd1f7c68))

- Fixed bug in init_config
  ([`ace2b2f`](https://github.com/bec-project/bec/commit/ace2b2f3377e7705bf4b200f1d74a73dbcaf98b0))

- Fixed bug in init_config
  ([`c9baaba`](https://github.com/bec-project/bec/commit/c9baaba7f225c5f784e10286e3ccd5c4da0368ed))

- Fixed bug in live_table callback; #closes 64
  ([`3c5e5b1`](https://github.com/bec-project/bec/commit/3c5e5b12997f54ec9d31d64877f61ac1efa876a0))

- Fixed bug in monitor scan
  ([`6fe0984`](https://github.com/bec-project/bec/commit/6fe0984fe007e81272568dbc116d1e1de45a4054))

- Fixed bug in primary devices
  ([`d675cf3`](https://github.com/bec-project/bec/commit/d675cf3264244515d8309e945ac5a36b0522e45b))

- Fixed bug in primary_devices
  ([`79a74b1`](https://github.com/bec-project/bec/commit/79a74b1b898e24a010ff8484112080c8472f1cb4))

- Fixed bug in readoutPriority propert; fixed device status for strings
  ([`52ce000`](https://github.com/bec-project/bec/commit/52ce0003e30bb4d0dee101b377a697c44f27a728))

- Fixed bug in scan worker max pointid assignment
  ([`d4cbead`](https://github.com/bec-project/bec/commit/d4cbead1decbcfdf968660f03699a4dc3c6bb3e3))

- Fixed bug in scans._run
  ([`2ea8c3e`](https://github.com/bec-project/bec/commit/2ea8c3e33b2b136e5de33b744b2bb003a9eb31c9))

- Fixed bug in scilog shutdown
  ([`c94613b`](https://github.com/bec-project/bec/commit/c94613b30c23fd7d89fac4c3378125109ea09376))

- Fixed bug related to queue refactoring
  ([`2949236`](https://github.com/bec-project/bec/commit/29492366e225c11e520751f60f41828eea1e8eb5))

- Fixed bugs related to refactoring
  ([`a45d683`](https://github.com/bec-project/bec/commit/a45d683f64bac1e5e62991a9d9afb66efdabb1fd))

- Fixed bugs related to refactoring
  ([`2f61b32`](https://github.com/bec-project/bec/commit/2f61b328cfb098e39f00f064bd1e4a17774d8e1c))

- Fixed callbacks for groups and scan defs; closes #32
  ([`ed41416`](https://github.com/bec-project/bec/commit/ed414167ad913cd22473c1da1b31d351e6e77ce1))

- Fixed client dependency
  ([`82e3384`](https://github.com/bec-project/bec/commit/82e3384cd2ddb5bda45376f9bf2a26d3f29a49e3))

- Fixed client dependency
  ([`bd81815`](https://github.com/bec-project/bec/commit/bd81815a7d3c7806d25664425f407e56a3396f1a))

- Fixed config
  ([`f7be217`](https://github.com/bec-project/bec/commit/f7be2172a17d97e3b67f5ba91762accb82839276))

- Fixed crash in exception
  ([`1f7e573`](https://github.com/bec-project/bec/commit/1f7e57362edf75daa6e69a1b557fe393d2cf7623))

- Fixed csaxs config
  ([`a03ae49`](https://github.com/bec-project/bec/commit/a03ae49cd2a862f96bed1df0dc762b654f27ec3c))

- Fixed default cSAXS config
  ([`95bc9a9`](https://github.com/bec-project/bec/commit/95bc9a9f949b49d30395ac620e2fd2c738b63030))

- Fixed default writer
  ([`73f555f`](https://github.com/bec-project/bec/commit/73f555f8457918251f46cef504268ddb7f5a95c8))

- Fixed dependencies
  ([`61c03fe`](https://github.com/bec-project/bec/commit/61c03fefa1a47a4c27ec87bd902d03b67db7c6cb))

- Fixed dependencies for bec_client_lib
  ([`b3ff495`](https://github.com/bec-project/bec/commit/b3ff49510571bdf912070b343a5336fc77ed7bdb))

- Fixed DeviceManager import
  ([`41a19b1`](https://github.com/bec-project/bec/commit/41a19b15fcdce6e0f58ccdf27936db7e738fdb6f))

- Fixed eiger1p5m config
  ([`fd795d8`](https://github.com/bec-project/bec/commit/fd795d8f95101d49b8fb5cbfb98a0f7ccc1ff2c7))

- Fixed empty device messages
  ([`8bd4d3d`](https://github.com/bec-project/bec/commit/8bd4d3d3fa4fb8eb12be19e79e9647b09842c503))

- Fixed encoding
  ([`8ca63da`](https://github.com/bec-project/bec/commit/8ca63da3153725f5fccac68edeab712aba34378a))

- Fixed feedback for queued scans
  ([`a5c16fe`](https://github.com/bec-project/bec/commit/a5c16feff717e75e793555cea9f600976645832a))

- Fixed file writer
  ([`7ff72f1`](https://github.com/bec-project/bec/commit/7ff72f12ddfa6ca0d197ee3ec31515c3d2b7dacc))

- Fixed file writer plugins
  ([`44e938e`](https://github.com/bec-project/bec/commit/44e938ee74e4b3337b5185283ef31b9bde64c395))

- Fixed fly scan motors
  ([`245e1c8`](https://github.com/bec-project/bec/commit/245e1c8d0e45417b852801e98d3982a8dcbda153))

- Fixed formatter
  ([`02b20a0`](https://github.com/bec-project/bec/commit/02b20a0ac8c064b6b2437a030c394c7722fc75bf))

- Fixed formatter
  ([`50d12f3`](https://github.com/bec-project/bec/commit/50d12f3961bf7ef305d8c4138d1610e638c3403a))

- Fixed formatter
  ([`b080060`](https://github.com/bec-project/bec/commit/b080060b2f9f9ed0bedad8f99bc50bf28c57db73))

- Fixed formatter
  ([`56f8a14`](https://github.com/bec-project/bec/commit/56f8a14b41dd7376ab1403e59ab167501f7edb82))

- Fixed gitignore
  ([`270dc82`](https://github.com/bec-project/bec/commit/270dc827300ba10157b0a7b72f684194cd50cc23))

- Fixed import bug
  ([`2813791`](https://github.com/bec-project/bec/commit/2813791ae795fccfc1459f09d6d63c47d80cf864))

- Fixed intermediate config write action
  ([`a48fff5`](https://github.com/bec-project/bec/commit/a48fff5f013be19914f18f1aef960ac649ddd03c))

- Fixed ip exception handler for ScanInterruptions
  ([`074ec13`](https://github.com/bec-project/bec/commit/074ec1362bd1dff4d634e55935b2aa6c1c511f41))

- Fixed kickoff for old interface
  ([`8d38101`](https://github.com/bec-project/bec/commit/8d38101c741ff9c44de16e98551ba99841ef8c39))

- Fixed kwargs forwarding for threaded consumer
  ([`5153847`](https://github.com/bec-project/bec/commit/5153847674a377e205909ff8a6dc3a01e4223017))

- Fixed logger for scan status paused and halted
  ([`c352ce0`](https://github.com/bec-project/bec/commit/c352ce0128c8c8bfc05268282fea221809cf5569))

- Fixed merge conflict
  ([`0b66cb0`](https://github.com/bec-project/bec/commit/0b66cb0ce5f9e41abfa2b32657deaaa1f2c495ee))

- Fixed message reader for bundled messages
  ([`4d7b141`](https://github.com/bec-project/bec/commit/4d7b141fb0c452eecf2f56330d3eacafb631923b))

- Fixed missing package
  ([`77a9bde`](https://github.com/bec-project/bec/commit/77a9bded34eac9529d392d424a1a9fecb1944f72))

- Fixed model
  ([`3475112`](https://github.com/bec-project/bec/commit/3475112ffa01d2d0e132faa7d8c849634a89c498))

- Fixed monitor scan update
  ([`0c468a5`](https://github.com/bec-project/bec/commit/0c468a5717884e319826f85ccd1f09a5c9ce963f))

- Fixed path to bec_config yaml file
  ([`9d18037`](https://github.com/bec-project/bec/commit/9d180372d122b439766666f13a2f474cb40a8ad2))

- Fixed path to config file
  ([`393db47`](https://github.com/bec-project/bec/commit/393db479363cfe359440e79f47f250cf8b3b5638))

- Fixed pre/post commit
  ([`88d151f`](https://github.com/bec-project/bec/commit/88d151fe2a56e1ff6e6e5d0729f1c324943c7b42))

- Fixed pre/post commit
  ([`2c3f36e`](https://github.com/bec-project/bec/commit/2c3f36ec8c60c8b424ad838d452eeb5eb3c2fe8b))

- Fixed primary_device list for overlapping scan motors
  ([`19b52ee`](https://github.com/bec-project/bec/commit/19b52eef9cf9fedb48eb5fc55cb5f26a329f462a))

- Fixed repr string
  ([`8dc2d6f`](https://github.com/bec-project/bec/commit/8dc2d6f61a9259b28aa9e3ec91674aa10cc1151f))

- Fixed rpc calls
  ([`183e5ae`](https://github.com/bec-project/bec/commit/183e5aefafa06ed04836f03b9f7249a666ee05f5))

- Fixed save_conig_file for locally created configs
  ([`81d82ed`](https://github.com/bec-project/bec/commit/81d82ed8977f0f417ac6e8807d7eec8e383c8a16))

- Fixed scan interruption message
  ([`b822655`](https://github.com/bec-project/bec/commit/b82265594cbaef68b13d6b1f3348315e91ec9221))

- Fixed scan object for client lib; closes #74
  ([`c285019`](https://github.com/bec-project/bec/commit/c28501993a7696bdadb63011c339f049ec60f2c7))

- Fixed serializer check
  ([`1d13e61`](https://github.com/bec-project/bec/commit/1d13e61fadaf7c9ec644f172c002a9b20a723df2))

- Fixed sls and cSAXS config for new db structure
  ([`6f898c6`](https://github.com/bec-project/bec/commit/6f898c602d81d966ee059d2fe1808ecdf83fa3a2))

- Fixed startup script after mixin changes
  ([`844cbc1`](https://github.com/bec-project/bec/commit/844cbc1e0f67b95d1aee7a6af16e8bb170898a94))

- Fixed startup script after mixin changes
  ([`e25c085`](https://github.com/bec-project/bec/commit/e25c085d29ca4999d8e9e12abaaff72466c6f76c))

- Fixed stdout and stderr redirect for tmux init
  ([`561a144`](https://github.com/bec-project/bec/commit/561a144fb9d74977d15cba6d698e2578ecf49457))

- Fixed sub devices info name
  ([`a37e057`](https://github.com/bec-project/bec/commit/a37e0572347c09bd1ecfeea8e08934cb20b3c836))

- Fixed sync scan item callbacks
  ([`1ae04ac`](https://github.com/bec-project/bec/commit/1ae04ac193ec9bb7a52cabf940a5aeb230288761))

- Fixed test for merged db updates
  ([`ad56d59`](https://github.com/bec-project/bec/commit/ad56d591e20bd5f8c616506104446d5bfa2e7bf5))

- Fixed tests
  ([`dbd2e73`](https://github.com/bec-project/bec/commit/dbd2e735a5886c55b1876baf3efe9f15cb956f5e))

- Fixed tests
  ([`018d781`](https://github.com/bec-project/bec/commit/018d781dbfa57209ab7cb5a6ab2b265a1b5df364))

- Fixed tomo scan file for repeated angles
  ([`d25d974`](https://github.com/bec-project/bec/commit/d25d9740f88540cc3de288314e0cb93ded7a2591))

- Fixed typo; fixed test
  ([`b2799f6`](https://github.com/bec-project/bec/commit/b2799f61f7ba927da766a36628804d4471537066))

- Fixed userParameter assignment
  ([`7dc43fa`](https://github.com/bec-project/bec/commit/7dc43fa29eff9fdf27bb7c97d7587095ca39ccbd))

- Fixed weird merge conflict
  ([`56bd317`](https://github.com/bec-project/bec/commit/56bd317e0f7c44b755c7de91638c2ff3027b4871))

- Fixed xrayeyealign init
  ([`a56a3a2`](https://github.com/bec-project/bec/commit/a56a3a2266f81a1866b994f0c491f6933386caef))

- Fixes after refactoring
  ([`7547f8d`](https://github.com/bec-project/bec/commit/7547f8d8c7cbeb3cf488f5673c1f1330ee7955e6))

- Fixes path to logbookmessage
  ([`c3bdee3`](https://github.com/bec-project/bec/commit/c3bdee3734acb4096221385f2d283896c3be3468))

- Improvements and fixes for redis config
  ([`ec5f915`](https://github.com/bec-project/bec/commit/ec5f9155ceac4d4df9eacd7ea5767d01d1c50029))

- Improvements for otf; added option to wait for kickoff
  ([`b685301`](https://github.com/bec-project/bec/commit/b6853012288a9fb3ebd991552cde8bed2ea7ddd8))

- Increased node version to 14.21
  ([`95e8e2a`](https://github.com/bec-project/bec/commit/95e8e2aefd98ac66b0b0e5034c313e61014643df))

- Lamni online changes
  ([`a11b09b`](https://github.com/bec-project/bec/commit/a11b09b027a3cb2d7012c4124980abdd83bcb4db))

- Left align bl_show_all output
  ([`a35aa06`](https://github.com/bec-project/bec/commit/a35aa0638405e01fd37f81cfcf523bb34eb8a33d))

- Moved flyer to hint-based
  ([`11fb30f`](https://github.com/bec-project/bec/commit/11fb30f64320f712ec93b24323079db7fe4831cb))

- Moved onFailure from acquisition config to root
  ([`72249ca`](https://github.com/bec-project/bec/commit/72249ca8730e4bcf4ab5e5863092faa8fe3ea4bc))

- Moved to separate release file
  ([`b29e191`](https://github.com/bec-project/bec/commit/b29e191453c3fe26d80ed9e705bba4e71857b73f))

- New scans clear the alarm stack
  ([`6cc2bcd`](https://github.com/bec-project/bec/commit/6cc2bcdeb3ea484c4057a22c469fd47fedac9a54))

- Online changes lamni
  ([`c0dba49`](https://github.com/bec-project/bec/commit/c0dba49377c16f54f000098c948b267a8d78979d))

- Only alarms are raised; warnings are logged
  ([`7b01c63`](https://github.com/bec-project/bec/commit/7b01c638a3aaf5c0b1056f1a80b2032628c7593e))

- Only pause if queue is not empty
  ([`deca3d1`](https://github.com/bec-project/bec/commit/deca3d1d77143379b559934e1307d2a85acea50a))

- Primary devices must be unique
  ([`0e4119e`](https://github.com/bec-project/bec/commit/0e4119ebefb6428ef78d6fd0b8ee5b24e358056c))

- Removed apk install; not needed for python image
  ([`0aa9ffc`](https://github.com/bec-project/bec/commit/0aa9ffcd7939ca049a860f666bf3b24ce96b1325))

- Removed bec_utils after refactoring
  ([`03d6a27`](https://github.com/bec-project/bec/commit/03d6a274d0ab26ea5f6536e952846e617d013d01))

- Removed duplicated reload call
  ([`db043ea`](https://github.com/bec-project/bec/commit/db043eaf47b983494ba8221ec646714e39d46665))

- Removed needs from semver job
  ([`88f0136`](https://github.com/bec-project/bec/commit/88f01360c48d26781b8a60c638abcd83a7ee4c08))

- Removed numpy dependency
  ([`f5b4a8a`](https://github.com/bec-project/bec/commit/f5b4a8a0ac1b6ba48ab817fb60101811b4dd4854))

- Removed on_failure from scan_worker as it is now handled for each device individually
  ([`544d6fe`](https://github.com/bec-project/bec/commit/544d6fec990e53a9d6ea03d8e52134b9ab4b699b))

- Removed scan_motor usage
  ([`52ff75a`](https://github.com/bec-project/bec/commit/52ff75a52502c78ee247b8bde2ce217c8275d58e))

- Removed wait for hidden reports
  ([`01c3850`](https://github.com/bec-project/bec/commit/01c38501084d5138988ea9a2c8338c2d1316ccc7))

- Renamed data_segment to scan_segment
  ([`59dfa4e`](https://github.com/bec-project/bec/commit/59dfa4e175bcea01db15af20afadb0400c20e834))

- Renamed deviceGroup to deviceTags
  ([`7e12750`](https://github.com/bec-project/bec/commit/7e127509db7cda8fcf4f0156c9a969d306cd22f7))

- Reset logbook info on init http error
  ([`e48c888`](https://github.com/bec-project/bec/commit/e48c8883a8dc4c26bad7e0a9daa2389fdba94a88))

- Reset version
  ([`564e599`](https://github.com/bec-project/bec/commit/564e59958a9083caf4bf64570835b4fc1263a6c1))

- Reverted black for json files
  ([`458e33a`](https://github.com/bec-project/bec/commit/458e33a0f5e1feac0a29b0bfe01ffbb81ef3a8ce))

- Skip None callbacks
  ([`0a60289`](https://github.com/bec-project/bec/commit/0a60289f0d0f3a5e23b683dafc316530010ab7b6))

- Subscriptions should only be added after the connection has been established
  ([`627cc45`](https://github.com/bec-project/bec/commit/627cc45570ec6cf61e80f78241ce672e85a1ab90))

- Testing semver publish
  ([`a0c70a5`](https://github.com/bec-project/bec/commit/a0c70a51e2ef619004d69161a118f77525e1804a))

- Unused wait groups are ignored
  ([`06cd3d5`](https://github.com/bec-project/bec/commit/06cd3d58c7c91da019af08ae9c23993d7d2c71e2))

### Continuous Integration

- Added bec_client_lib to ci tests
  ([`2c50442`](https://github.com/bec-project/bec/commit/2c504421bc97f3afa1d7a14ba9a8d4bffbd1ddaa))

- Added bec_client_lib to ci tests
  ([`af13f0b`](https://github.com/bec-project/bec/commit/af13f0b3868897ce8f397d526cb07bc4b1a6f98e))

- Added end2end-light
  ([`6dfb75b`](https://github.com/bec-project/bec/commit/6dfb75b88526fcf904aadfd840b99e2a7be77e33))

- Added logger to waiting function
  ([`5f9345a`](https://github.com/bec-project/bec/commit/5f9345ae6ff00ab655ac49c38e2f685da2fb0aca))

- Added needs
  ([`58d77e2`](https://github.com/bec-project/bec/commit/58d77e2f6e48adfb96feb4c1877f475939f2890b))

- Added pipeline report
  ([`9b3cde4`](https://github.com/bec-project/bec/commit/9b3cde4f62c74039c19212f051f39d3d9aff11b1))

- Added pytest parallel
  ([`ba3d52f`](https://github.com/bec-project/bec/commit/ba3d52ff31824682a07717e3a9af0d1bc9d41e9b))

- Added pytest parallel
  ([`20e0549`](https://github.com/bec-project/bec/commit/20e054907d1363ce5ef413ae68dfc79ae2d7e930))

- Added python3.11 tests; added end2end tests without scibec
  ([`8a538de`](https://github.com/bec-project/bec/commit/8a538de290712eed482e8a274ab9d92efeae9b67))

- Added tests for different python versions
  ([`6af39af`](https://github.com/bec-project/bec/commit/6af39af1648397a8ca21c9cd3e4a977729dcecac))

- Cleanup
  ([`ecfd39e`](https://github.com/bec-project/bec/commit/ecfd39e33e95020172c80203cac65b13cc4bb021))

- Fixed bug in ci file
  ([`4dae74f`](https://github.com/bec-project/bec/commit/4dae74f5642b84b3e64f37a37c7746b27373bfb0))

- Fixed build order
  ([`ec5b0a3`](https://github.com/bec-project/bec/commit/ec5b0a351f8a025ffb37ba9522a13ce19c5b2bce))

- Fixed dockerfile
  ([`c263548`](https://github.com/bec-project/bec/commit/c263548947a4f8632b698f590c72ef30a63f66f0))

- Fixed dockerfile
  ([`5a1e5ba`](https://github.com/bec-project/bec/commit/5a1e5baca0e71dd5bc42ada493c430730a296b75))

- Fixed path to test_config
  ([`0120ccc`](https://github.com/bec-project/bec/commit/0120ccca26919861d2f0a59f86643a62fe4245ea))

- Fixed path to test_config
  ([`04613a7`](https://github.com/bec-project/bec/commit/04613a7bf0db86d7568b0712fd156bd45fb9aae9))

- Fixed python image
  ([`dfa9cdf`](https://github.com/bec-project/bec/commit/dfa9cdfc0630af6feb6f3cbcebb95e531c9e1831))

- Fixed test init
  ([`732995a`](https://github.com/bec-project/bec/commit/732995af833de8a2e8d1211284b88d09abae2ee1))

- Fixed typo
  ([`f970c97`](https://github.com/bec-project/bec/commit/f970c9785c13db19e1a446f0a4b3116de315d6ce))

- Moved from alpine to 3.8
  ([`8e5464b`](https://github.com/bec-project/bec/commit/8e5464bfeeb6c75abf87c8e4959701134be41e1e))

- Moved requirements to requirements.txt file
  ([`411a310`](https://github.com/bec-project/bec/commit/411a310e65b67d7489e7cecc8996108ef7f3ff59))

- Removed light end2end for now
  ([`33b2e76`](https://github.com/bec-project/bec/commit/33b2e7694bb05038eb561380cbb24cf438e2a144))

- Removed ophyd_devices branch
  ([`61fae3c`](https://github.com/bec-project/bec/commit/61fae3c79563306a24b2ebb827984aa046d67810))

- Removed pytest parallel
  ([`f732736`](https://github.com/bec-project/bec/commit/f73273662ade38e3dfb76ceed9f378cbf1ed168d))

- Update openapi schema file during the pipeline
  ([`079a3e0`](https://github.com/bec-project/bec/commit/079a3e06c82acf50d3fa6d3cf626d755c02cfba7))

- Upgraded docker and dind version to 23
  ([`7e0c73a`](https://github.com/bec-project/bec/commit/7e0c73aa9445e09fc2e812c13b6c594ee19a1c6f))

### Documentation

- Added bec architecture
  ([`5be499c`](https://github.com/bec-project/bec/commit/5be499cc8ed0cc40198ef4286cbb5ce43b149b93))

- Added bec context image
  ([`3b1d9e7`](https://github.com/bec-project/bec/commit/3b1d9e7f9fb4d1331d54f6672548c35575104207))

- Added bec context image
  ([`581792c`](https://github.com/bec-project/bec/commit/581792c674c90709edd18349c994d1b8e95eb87a))

- Added missing file
  ([`f8fa8f4`](https://github.com/bec-project/bec/commit/f8fa8f4136068b2370c8f63c23eb29c1cbd84a39))

- Added version switcher
  ([`eaaff10`](https://github.com/bec-project/bec/commit/eaaff100a12e888b5904a5709e20f00c522c44d2))

- Cleanup
  ([`1381f74`](https://github.com/bec-project/bec/commit/1381f74e43bcd74302f4a5827f4897c8067e1a8e))

- Fixed requirements formatting
  ([`e6beda3`](https://github.com/bec-project/bec/commit/e6beda3b9c8d634efa1f855ca94fb91d167d4a95))

- Fixed requirements formatting
  ([`783d406`](https://github.com/bec-project/bec/commit/783d406eea778228898e0070a0a31b5847346494))

- Improved doc string for lamNI move to scan center
  ([`6cc3e51`](https://github.com/bec-project/bec/commit/6cc3e51d57565076d4d47f7e5e150b828fa45475))

- Removed switcher
  ([`c802d9e`](https://github.com/bec-project/bec/commit/c802d9e40809388f940422d159095f3a3bd62c11))

- Update drawings
  ([`40459f6`](https://github.com/bec-project/bec/commit/40459f67c09bdc965c34678af485ad4943bca460))

- Updated BEC c4 drawing
  ([`7ccb80d`](https://github.com/bec-project/bec/commit/7ccb80d2d32ae3871b35b482ec818d778108ac8a))

- Updated config db drawing
  ([`e39c3f4`](https://github.com/bec-project/bec/commit/e39c3f489eb4d47d0709be27accfc25d63c98e1c))

- Updated docs for new redis config
  ([`e88fa30`](https://github.com/bec-project/bec/commit/e88fa309c7806f3a6488728a5463434373d4dd55))

- Updated drawings
  ([`009ed3b`](https://github.com/bec-project/bec/commit/009ed3b830fd648ad98929db3d4194feca6b1be7))

- Updated drawio
  ([`4b7981e`](https://github.com/bec-project/bec/commit/4b7981e9cf3c9ca1d234f4679710e2d7d8cbde44))

- Updated instructions for creating a new config file
  ([`0491837`](https://github.com/bec-project/bec/commit/0491837041323afe71d64bc2971207741dab7145))

- Updated tutorial
  ([`0a8b7dc`](https://github.com/bec-project/bec/commit/0a8b7dc5268adbd47e5c2b5a48c79fc53ba890c6))

### Features

- Active account is now pulled from redis
  ([`0837bc8`](https://github.com/bec-project/bec/commit/0837bc8959a81be4e08952d332b7e05ca0e1f14f))

- Added _raised_alarms
  ([`ae5ba37`](https://github.com/bec-project/bec/commit/ae5ba37716634705d4abcd621c6fca3a04f65356))

- Added access to optics mixin from align
  ([`4089f35`](https://github.com/bec-project/bec/commit/4089f35d5ea97fb4ef0bfc12295addf1da45b696))

- Added acquisition priority
  ([`13927dc`](https://github.com/bec-project/bec/commit/13927dc8d6d872f0bb0893d2d19001edd3df765d))

- Added baseline endpoint
  ([`f630272`](https://github.com/bec-project/bec/commit/f630272fee3b67f65addb897e5c15f3ff6395595))

- Added baseline publisher
  ([`165e59e`](https://github.com/bec-project/bec/commit/165e59e1948d521d7ac660e0c705a49c697acd76))

- Added beamline info
  ([`f80de69`](https://github.com/bec-project/bec/commit/f80de691d6be48cc02ae1cd4c2a151f35de030ce))

- Added bec cli command; added bec_startup script
  ([`15e71e0`](https://github.com/bec-project/bec/commit/15e71e073226f3eb895f7a8829dafffc7a0071a5))

- Added bec metrics
  ([`a3fa79b`](https://github.com/bec-project/bec/commit/a3fa79bb9fa39c5ed6b6d3f8b8b80c1f922fd5c1))

- Added becmessage version 1.1; added option to select compression
  ([`0de43a8`](https://github.com/bec-project/bec/commit/0de43a800f916b61c393384c038ef3425e3b9cc5))

- Added complete and support for new kickoff signature
  ([`c7c1c44`](https://github.com/bec-project/bec/commit/c7c1c44be608f9d96b594fc2098f91cf5a0562bc))

- Added config action set
  ([`511cbcb`](https://github.com/bec-project/bec/commit/511cbcbf4072011cd05ae0fe9a80977c08aa4a29))

- Added console log; added target account property
  ([`7a77429`](https://github.com/bec-project/bec/commit/7a77429a4cebb7c10a1100dbe291ed057089d33d))

- Added customized alarm handling for bec errors in ipython
  ([`fa9723b`](https://github.com/bec-project/bec/commit/fa9723b28230cb0d64815b75d4295dedf76f62cd))

- Added device description to device report
  ([`5b34204`](https://github.com/bec-project/bec/commit/5b34204336349a255b4df3a17b98b81dba4adaf6))

- Added device_schema
  ([`8b26e81`](https://github.com/bec-project/bec/commit/8b26e8153251ba06ad7bf471826f9f42ad28c1aa))

- Added emitter log
  ([`07bbaa0`](https://github.com/bec-project/bec/commit/07bbaa05f325cc7aed25c19531b130ebd2ec3840))

- Added event callback
  ([`9e11f66`](https://github.com/bec-project/bec/commit/9e11f66d8040052f7d36269b2cecce32d02eae5d))

- Added event controller
  ([`e25d077`](https://github.com/bec-project/bec/commit/e25d0773c9e87b6eaf2e239617668b7ffc0c83ad))

- Added event publisher to scihub
  ([`a2efb91`](https://github.com/bec-project/bec/commit/a2efb91d1b57945418bac6650bb6c014a5047063))

- Added flyer event and callback
  ([`636ee79`](https://github.com/bec-project/bec/commit/636ee7950080db477a97acc3c9da6b696988ada5))

- Added hyst_scan; online changes
  ([`ff5706c`](https://github.com/bec-project/bec/commit/ff5706c03dc001374055c1985fe4f44af005c494))

- Added linter to check the imported user scripts
  ([`6cc2e6a`](https://github.com/bec-project/bec/commit/6cc2e6a700509e4ce9e30339ff603b19423e0eb5))

- Added list scan
  ([`32fab04`](https://github.com/bec-project/bec/commit/32fab041a2a4bf81b6f082187638415407cbd56a))

- Added logbook and account endpoints
  ([`ae13604`](https://github.com/bec-project/bec/commit/ae13604b344f40ad86377f36d46519db957768ab))

- Added logbook message for beam checks
  ([`f72881d`](https://github.com/bec-project/bec/commit/f72881d66ec09254afd54b294b56bfb169bf5872))

- Added metadata to callbacks
  ([`1a14e00`](https://github.com/bec-project/bec/commit/1a14e0000eaced0eefcbf14b41cc0bfee2fe3d2d))

- Added monitor scan
  ([`8ac476e`](https://github.com/bec-project/bec/commit/8ac476e178979f5091e7a9c918fecb80756999b8))

- Added multiple iterations for corridor optim
  ([`1dff48e`](https://github.com/bec-project/bec/commit/1dff48ea049948614dfe269f5588eeac8b3495d1))

- Added on_failure options to device_server
  ([`b96a931`](https://github.com/bec-project/bec/commit/b96a93141f2ed8077864119605fe87f45daba421))

- Added on_failure updates to device_manager
  ([`ac3cfaa`](https://github.com/bec-project/bec/commit/ac3cfaa5599be8182f898055d90404b6e417a5ca))

- Added option to change the readout priority
  ([`28e1500`](https://github.com/bec-project/bec/commit/28e15001fc94d8c26ff79e665e870b22b1a2d4a1))

- Added option to hide the table and only show the progressbar
  ([`c0d76b1`](https://github.com/bec-project/bec/commit/c0d76b1c5af46d3b366089b5ed0551d813a24cff))

- Added option to override the singleton client
  ([`db0f2f7`](https://github.com/bec-project/bec/commit/db0f2f7864323feadcb8bbcf56614682316ec692))

- Added option to skip the signal filtering
  ([`a593424`](https://github.com/bec-project/bec/commit/a593424fe2815e42f8315dbce91aa43a797e4b7f))

- Added otf scan
  ([`09f636e`](https://github.com/bec-project/bec/commit/09f636e61849c6f1090198d98e9571adba3e457e))

- Added phase plates
  ([`97ea86c`](https://github.com/bec-project/bec/commit/97ea86c710de0738951f0d6889e2a4cfcdd55817))

- Added readout_priority as replacement for scan_motors
  ([`adedc1b`](https://github.com/bec-project/bec/commit/adedc1b83bf45f888092538c51bd778f47f09dc1))

- Added scan callbacks
  ([`3ebc910`](https://github.com/bec-project/bec/commit/3ebc9108928e3a753f7db4c1cf39bcf866f372f3))

- Added scan history
  ([`cb3ce8b`](https://github.com/bec-project/bec/commit/cb3ce8b91b5d8cac445baef8d2182395b0277dbc))

- Added scan report instructions
  ([`35cd892`](https://github.com/bec-project/bec/commit/35cd892d2bdbaa5922bad1c52bf13bef7f99e21c))

- Added scan_item repr
  ([`b611fbb`](https://github.com/bec-project/bec/commit/b611fbbda2abc1ceae57b3d78f75c69889c64eaa))

- Added scan_report_devices
  ([`558416a`](https://github.com/bec-project/bec/commit/558416a2f8530d9df2147f5c8f415f12bacca746))

- Added scihub service
  ([`bbabee1`](https://github.com/bec-project/bec/commit/bbabee1c0c76433b0cfa1e967ebcce55f62739cd))

- Added scilog export for lamni
  ([`f6def4f`](https://github.com/bec-project/bec/commit/f6def4f6ff86130be7efb831e45497c65422086e))

- Added scilog to scihub
  ([`7c297b3`](https://github.com/bec-project/bec/commit/7c297b32d85c5822a95665cdf3e6f3d800d1ae1c))

- Added scoped import for ophyd devices
  ([`e644c19`](https://github.com/bec-project/bec/commit/e644c19e061631086386e184b21545e5afdcf5e7))

- Added script to test the validity of configs
  ([`2958427`](https://github.com/bec-project/bec/commit/2958427d64381c7e7f49381cb0251342993cdc38))

- Added semver log to pre-commit
  ([`b9f6bb5`](https://github.com/bec-project/bec/commit/b9f6bb575921251f8d422ff6dcedb25a9a26af9a))

- Added semver to ci
  ([`672a6ad`](https://github.com/bec-project/bec/commit/672a6adef4060e92882e907c1f329ebce90972da))

- Added settling time; fixed burst_at_each_point; removed exp_time as req kwarg
  ([`12ae5c7`](https://github.com/bec-project/bec/commit/12ae5c715ef4bfda3fb19273f51a49cf7f5df251))

- Added show_all; minor improvements
  ([`03e63ce`](https://github.com/bec-project/bec/commit/03e63cef167acce0764b4162ced7cee9022d5a66))

- Added show_tags
  ([`45dcf76`](https://github.com/bec-project/bec/commit/45dcf767a0b7815d5e3afda57a23e4a940e1cc5e))

- Added support for device tags
  ([`d00a81d`](https://github.com/bec-project/bec/commit/d00a81dde7920cdc02c0a1ea5c7315716e2ef195))

- Added support for intermediate scan_report_instructions
  ([`03027f3`](https://github.com/bec-project/bec/commit/03027f3ae02cff96492bdda1a4ea05d359e7a9a8))

- Added support for nested dataset id cms
  ([`ada6493`](https://github.com/bec-project/bec/commit/ada649387b3abd9dba5b53319f11c4ad195f477a))

- Added support for new scibec structure
  ([`b65259d`](https://github.com/bec-project/bec/commit/b65259d4492a0fe9dc7efba577dec80fc3b65327))

- Added support for rpc status
  ([`87716db`](https://github.com/bec-project/bec/commit/87716dbbafedc8f69210b9aa9da927653a0593b1))

- Added support for rpc status return values
  ([`1fcd6ff`](https://github.com/bec-project/bec/commit/1fcd6ffb25c43e9fc6e17b1e6628232c232b3727))

- Added test config cSAXS
  ([`539ae01`](https://github.com/bec-project/bec/commit/539ae01a64eb02cd00fab695e78929aa34456398))

- Added time scan
  ([`e77c5a6`](https://github.com/bec-project/bec/commit/e77c5a6bb0da5f7bc071655132768e46b090c528))

- Added user functions to show all commands
  ([`8259cd3`](https://github.com/bec-project/bec/commit/8259cd3a5488f51c527dd29bab3451d9cb36a629))

- Added user params to lamni config
  ([`48c9f94`](https://github.com/bec-project/bec/commit/48c9f94b545a76d1ccf1dfab444534859a3f4835))

- Added wm
  ([`7a29d43`](https://github.com/bec-project/bec/commit/7a29d43383bc9224560919c46d232ef242760fa8))

- Added xtreme plugin
  ([`342c2a6`](https://github.com/bec-project/bec/commit/342c2a6f148fe26ca9d160576cca1e2871e324fe))

- Decoupled scibec from core services; added config to redis
  ([`d846d55`](https://github.com/bec-project/bec/commit/d846d55cde40b124db7756caadcaa36968d034f9))

- File writer raises minor error when file is not written
  ([`3800917`](https://github.com/bec-project/bec/commit/3800917695eca93738ab42f0704df46b5692bb07))

- First version of bec_client_lib
  ([`cd58b13`](https://github.com/bec-project/bec/commit/cd58b1358ca4cc9e0855b6c1ec0034ab1fcfd14f))

- First version of bec_client_lib
  ([`d66149d`](https://github.com/bec-project/bec/commit/d66149db27ed60e7656c4d98a994db1779c85bd7))

- Improved becmessage repr for easy loading from string
  ([`acc5949`](https://github.com/bec-project/bec/commit/acc59499532e0a415c08e70b7a45b87cf7b72022))

- Improved device info
  ([`1fd83d4`](https://github.com/bec-project/bec/commit/1fd83d4c6a8e3937425e120b12e446b5ff6cde17))

- Made emitter modular
  ([`63cc4ff`](https://github.com/bec-project/bec/commit/63cc4ffe93351ca1a808eed1ec16f6f7f61add43))

- Moved beam checks to redis
  ([`69e8a2b`](https://github.com/bec-project/bec/commit/69e8a2bdbd9fd3de60351d0ae308f4b34de59ef9))

- Online backend changes to support scan and dataset entries
  ([`06be92c`](https://github.com/bec-project/bec/commit/06be92c21d8fec6488d65e97a10525604e037cfc))

- Upgraded scibec to new db structure
  ([`84d76d3`](https://github.com/bec-project/bec/commit/84d76d3a6d3e19c81c97a3185785cbcc30ce84df))

### Refactoring

- Added builtins to avoid pylint errors
  ([`7c9bae4`](https://github.com/bec-project/bec/commit/7c9bae455d41773258b5da23b94cf63c792f8b94))

- Added mixin class for redis consumer
  ([`6e81164`](https://github.com/bec-project/bec/commit/6e81164bc06e80172bc2e5a24a15ded2018a6444))

- Added on_cleanup emitter
  ([`8791ca5`](https://github.com/bec-project/bec/commit/8791ca572555ca0b83f25f973fb0801165e44f3d))

- Added option to specify the redis cls
  ([`13a672d`](https://github.com/bec-project/bec/commit/13a672dc4a59fa1fb05cbc9753c8dd454c9e92a9))

- Added raise alarm / warning to failed readings in ds
  ([`f71c494`](https://github.com/bec-project/bec/commit/f71c494123094fe3482c15a117761e571736e22b))

- Added support for bl_show_all plugins
  ([`737c386`](https://github.com/bec-project/bec/commit/737c386e24618171d1cb4ef77935bdc44a968507))

- Changed bl_show_all from inheritance to composition
  ([`b2e57e7`](https://github.com/bec-project/bec/commit/b2e57e7d987d9c09e25fa29afadc0aa2be240747))

- Changed bl_show_all from inheritance to composition
  ([`fb40a61`](https://github.com/bec-project/bec/commit/fb40a61e77e6d38059cddecbfea295ac6c04db5f))

- Changes after rebase
  ([`4515618`](https://github.com/bec-project/bec/commit/45156183b6e51190ddba11419b3d301dd51b7ef4))

- Changes related to new config management
  ([`d0134ca`](https://github.com/bec-project/bec/commit/d0134ca4beca24206a20356cc43c3a065a9e5b94))

- Cleanup
  ([`a651567`](https://github.com/bec-project/bec/commit/a651567506a3828f09d7f3c0404c6305ef64b92d))

- Cleanup
  ([`b730fbf`](https://github.com/bec-project/bec/commit/b730fbf3b87733813c33edb9632b1a9f6b2dd8df))

- Cleanup
  ([`893bc19`](https://github.com/bec-project/bec/commit/893bc1909a731f5add76b7e7d3dfc7be443a0ec6))

- Cleanup
  ([`0df7811`](https://github.com/bec-project/bec/commit/0df7811680777fe860b24d5a434b337600cd54af))

- Cleanup
  ([`d2459c9`](https://github.com/bec-project/bec/commit/d2459c9a628420a69cadf0ad8fab1e6b697875b1))

- Cleanup
  ([`8ebb7b2`](https://github.com/bec-project/bec/commit/8ebb7b2430844e56855c34b70486f59d281d76bc))

- Cleanup
  ([`cfd3b19`](https://github.com/bec-project/bec/commit/cfd3b19b499af2161a657257e593b2dd71e354e4))

- Cleanup
  ([`cd7d63a`](https://github.com/bec-project/bec/commit/cd7d63a23b0f9463393dfabce5fa7da925c09cd9))

- Cleanup
  ([`8f9f865`](https://github.com/bec-project/bec/commit/8f9f8659298662744a39aa0d6e3b12b8b7307b82))

- Deviceinstructionmessage should always have at least one device
  ([`8f3d6df`](https://github.com/bec-project/bec/commit/8f3d6df6a1c6b0130896071aad2418ca24fd30b1))

- Fixed formatter
  ([`de81c30`](https://github.com/bec-project/bec/commit/de81c3092483c4c618556a5ad858366ce222c6ef))

- Fixed formatter
  ([`f749900`](https://github.com/bec-project/bec/commit/f7499005a9bac7533007c6d4b302b91e989afa97))

- Fixed formatting
  ([`9c72f89`](https://github.com/bec-project/bec/commit/9c72f89144f58d5c9b08c6bf013a868693fda247))

- Improved log messages for scanstatus
  ([`ce8300f`](https://github.com/bec-project/bec/commit/ce8300f702253b32e33d02b08122bc6f816bac5e))

- Improvements for bluesky emitter
  ([`84310b8`](https://github.com/bec-project/bec/commit/84310b8ef61f4dcccf83f9960b67dbd2c25d4dfa))

- Merged config updates
  ([`62420f3`](https://github.com/bec-project/bec/commit/62420f30a2c97636e1cc40cbfe1a6ee36286539a))

- Merged consumer and threaded consumer
  ([`0649f56`](https://github.com/bec-project/bec/commit/0649f56b9dea3f91289d3c45eba54aba5b243d64))

- Merged send_logbook_message and send_message to always send the linkType
  ([`e082ee6`](https://github.com/bec-project/bec/commit/e082ee6a4b2ea05108c4f724a7f518ff7b6770e3))

- Minor refactoring
  ([`98c870a`](https://github.com/bec-project/bec/commit/98c870a662fa3ad72b7819296f3568eb7e134e1a))

- Minor refactoring
  ([`7ded49c`](https://github.com/bec-project/bec/commit/7ded49c65bf390b26d00b8d9d20b883f3e530067))

- Minor refactoring for testing
  ([`f8c0ef6`](https://github.com/bec-project/bec/commit/f8c0ef620adbf7691610a43a05be2369e93636b8))

- Moved config_helper from client to utils
  ([`5d326f9`](https://github.com/bec-project/bec/commit/5d326f96eb05ab7f5558e153af0f7ebdfc3f4bae))

- Moved emitterbase to new file
  ([`dce87ab`](https://github.com/bec-project/bec/commit/dce87ab157af23107d8b5d0c96ef4e70694b391f))

- Moved tests from utils to bec_client_lib
  ([`a47a75e`](https://github.com/bec-project/bec/commit/a47a75e3917a45f4d8b43857979ce2c0b4a8e016))

- Moved utils into bec_client_lib
  ([`f43d4eb`](https://github.com/bec-project/bec/commit/f43d4ebac68e642eb8c151ebd6afb0d62709f249))

- Moved wait_for_empty_queue to utils
  ([`73e7a64`](https://github.com/bec-project/bec/commit/73e7a649855bb95c437ff707cd1223b2ee7f61ff))

- Only pass msg, not messageobjects
  ([`6a3d387`](https://github.com/bec-project/bec/commit/6a3d387131e400e4bc3deab1f0a8d441e7af0610))

- Refactored device list extraction
  ([`f77bd80`](https://github.com/bec-project/bec/commit/f77bd805694ab7a26b2ab0e10720d37006dbbeac))

- Refactored emitterbase
  ([`d8a0d6a`](https://github.com/bec-project/bec/commit/d8a0d6ad94dd8a29146edf5829ba8456605dae82))

- Refactored scan bundler
  ([`6583509`](https://github.com/bec-project/bec/commit/6583509fa91f33d0de0fa015e93a1ca59cce0ebe))

- Refactoring for live updates
  ([`36e66b6`](https://github.com/bec-project/bec/commit/36e66b660045054a02e86c14043a87308c626d6d))

- Removed DeviceManagerDeviceServer
  ([`6a04837`](https://github.com/bec-project/bec/commit/6a04837697ba23e72af8c230fb5b77a8a99b7a02))

- Removed DeviceManagerSB
  ([`2e7321d`](https://github.com/bec-project/bec/commit/2e7321d2d0758b34b7c3418c76d596b65fd5a8fa))

- Removed influxdb forwarder
  ([`9a5c003`](https://github.com/bec-project/bec/commit/9a5c0035964e0f9a6fd4f04526eb5fb15027f859))

- Removed pipeline
  ([`66b329e`](https://github.com/bec-project/bec/commit/66b329eaf6b29b293cf6a07e50586d8933cf17c3))

- Removed remaining bec_utils files
  ([`7604797`](https://github.com/bec-project/bec/commit/7604797b32e40bd29be2c53750ce0809a916db40))

- Removed unused endpoint device_last_read
  ([`97e09f0`](https://github.com/bec-project/bec/commit/97e09f0a5545275f2922bc96dbacfad2fb0b1ddc))

- Removed unused import
  ([`8c1ec1e`](https://github.com/bec-project/bec/commit/8c1ec1e637183527da3e55899620d0e863661ad7))

- Renamed ipython_live_updates
  ([`53f2876`](https://github.com/bec-project/bec/commit/53f2876f7fa004545de77a47850581c45a1d10b0))

- Renamed priority to readoutPriority
  ([`abe358d`](https://github.com/bec-project/bec/commit/abe358d956a699318503c22f1213b69d7b572969))

- Renamed readoutPriority enums
  ([`c0873aa`](https://github.com/bec-project/bec/commit/c0873aae87549921aa49d2c08963a991f387d67a))

- Replaced epylint by pylint.run
  ([`20f3804`](https://github.com/bec-project/bec/commit/20f38044a27e0f42d16eb5f602cfb17a42bd7bdc))

- Replaced jsonschema by fastjsonschema
  ([`d729fc2`](https://github.com/bec-project/bec/commit/d729fc2b255b5e4ed3ae431a504be60d57f4aca8))

- Simplified assert_device_is_enabled; added assert_devices_is_valid
  ([`0739de1`](https://github.com/bec-project/bec/commit/0739de1d1f1272c6efec2173a4ed6ee38b4643b0))

- Upgraded to black 23.1
  ([`989dd1f`](https://github.com/bec-project/bec/commit/989dd1fd8742c1f143e5885b5c2dce1763074edb))

- Upgraded to black 23.1
  ([`49b938d`](https://github.com/bec-project/bec/commit/49b938d391f66731ca4785a757678a4bb93e1a0a))

- Upgraded to black 23.1
  ([`36e5965`](https://github.com/bec-project/bec/commit/36e5965d61a081f2ce0edf4d4f86f92a6b6af6db))

- Use raise_alarms instead of looping through alarms
  ([`35784f3`](https://github.com/bec-project/bec/commit/35784f37c38f9641de7a2bff6641241c43538456))

### Testing

- Add test_assert_device_is_valid
  ([`0b35a4c`](https://github.com/bec-project/bec/commit/0b35a4c66926b1e0aef8f2b3b08e5a851534fd86))

- Added callback tests
  ([`4f547a7`](https://github.com/bec-project/bec/commit/4f547a7dac5db2533dd4a59ceabd41dff297bf17))

- Added callback_handler tests
  ([`58b347e`](https://github.com/bec-project/bec/commit/58b347e0951eb6f5f43ce6337dc706d76447a1d2))

- Added cleanup test
  ([`1826f98`](https://github.com/bec-project/bec/commit/1826f982ddbf07a88021244582ec1775f154adcf))

- Added config_handler tests
  ([`0848413`](https://github.com/bec-project/bec/commit/0848413d3aa4e87e54abe861790da726a48032ce))

- Added device server tests
  ([`488654e`](https://github.com/bec-project/bec/commit/488654e3a622bda2e58ef0aa4383cdd53207453e))

- Added emitter tests
  ([`3b47a83`](https://github.com/bec-project/bec/commit/3b47a838b18ea0a3307623bd83c31137b6156fa8))

- Added more scihub tests
  ([`f68d854`](https://github.com/bec-project/bec/commit/f68d85425e8a67eb64ec6905e8845f788fe9a912))

- Added more tests
  ([`d84cc6b`](https://github.com/bec-project/bec/commit/d84cc6b0ba90e8aa1b65b1b66897c70f8693a18e))

- Added more tests
  ([`9b9bd59`](https://github.com/bec-project/bec/commit/9b9bd5973b969bf633579618e8c24eaa4f27de7e))

- Added num_iterations for path optim
  ([`d94459c`](https://github.com/bec-project/bec/commit/d94459c615c6d5c491e533f562827a5d05b68bec))

- Added optim_trajectory to fermat spiral test
  ([`7ad4a11`](https://github.com/bec-project/bec/commit/7ad4a11c1c08a0fa9b96c53a3005ffdbe4d83f4d))

- Added option to discard pipeline data
  ([`c44da88`](https://github.com/bec-project/bec/commit/c44da888014d9a9bc4feb2b9b6156c6841e3b570))

- Added scan bundler test
  ([`ceccbf5`](https://github.com/bec-project/bec/commit/ceccbf534eac9da61c0973e71eb1086b6cb59611))

- Added scan guard tests
  ([`a94e53e`](https://github.com/bec-project/bec/commit/a94e53e047035bcc3ec9a681eb46985ddf419ce2))

- Added ScanObject tests
  ([`aa63ffb`](https://github.com/bec-project/bec/commit/aa63ffb2c5fb65cf6466701fc4f599189b780015))

- Added sleep to wait for limit updates
  ([`2eb9a36`](https://github.com/bec-project/bec/commit/2eb9a36e994c214007e63d94986f4d03e205b910))

- Added test for bl_info
  ([`5f06dcc`](https://github.com/bec-project/bec/commit/5f06dcc7340df41d6141e7e435d2c30bb1206410))

- Added test for emitter
  ([`01a9a6f`](https://github.com/bec-project/bec/commit/01a9a6f0e6f40b17ff06f279e80cb57d5abc4e74))

- Added test for test_step_scan_update when there is no pointID
  ([`d4d9e76`](https://github.com/bec-project/bec/commit/d4d9e76dc886e60fb62e1209c566ed75a8edb7d6))

- Added test_assert_device_is_enabled
  ([`4d091b8`](https://github.com/bec-project/bec/commit/4d091b8fdf60004e0816c96d3d3aede3f8df91a9))

- Added tests for bec emitter
  ([`7984ef8`](https://github.com/bec-project/bec/commit/7984ef8c3fca532438cd4df4dac885c2f33b5423))

- Added tests for tags
  ([`4f86f62`](https://github.com/bec-project/bec/commit/4f86f626ad4258fb0f1a006f27036dd769b0a1f6))

- Added tests; added doc strings
  ([`2b51ceb`](https://github.com/bec-project/bec/commit/2b51cebe1c4d7b140449bbbe034b5b05bf559253))

- Added x-ray-eye align tests
  ([`bffb734`](https://github.com/bec-project/bec/commit/bffb7341ca9de908d29f67caff5a3b019d53d7d5))

- Changed from magicmock to context manager
  ([`32e1f3b`](https://github.com/bec-project/bec/commit/32e1f3b5594076aecaebcee4168a867cf97aa4ad))

- Changed from magicmock to context manager in more tests
  ([`6d31eb5`](https://github.com/bec-project/bec/commit/6d31eb57d4218e8903fcbb54b55b70479714f43f))

- Cleanup
  ([`2cf693e`](https://github.com/bec-project/bec/commit/2cf693e141b5ae1fae91bd6d8406e6d1e012f6e5))

- Cleanup
  ([`8256ded`](https://github.com/bec-project/bec/commit/8256dedb77ca1b996b69f819631b0436933af5bc))

- Disabled metrics and service info threads for tests
  ([`28d26b6`](https://github.com/bec-project/bec/commit/28d26b6c39416e9da556f337dff42dc26e273e61))

- Disabled service info and metrics messages for mocked scan server
  ([`41fcde8`](https://github.com/bec-project/bec/commit/41fcde8df7aa5da7016091f45ea13a73b44b1c35))

- Ensured that scan id changes for different test cases
  ([`4cf853b`](https://github.com/bec-project/bec/commit/4cf853b5b612a8c89687836a7b787ceee9d54c58))

- Expanded test_trigger_device, rearranged the order of the tests
  ([`4715628`](https://github.com/bec-project/bec/commit/4715628c9dd82e89854111ac0bf53615565c2430))

- First test for test_get_device_status
  ([`8bc0ae2`](https://github.com/bec-project/bec/commit/8bc0ae27675c6e9ef024276373ca7dadbcb008a0))

- First tests for redis_connector
  ([`4791e10`](https://github.com/bec-project/bec/commit/4791e104378da559393667d6c2ac91a2ff7b1e8e))

- Fixed bec client import for end2end tests
  ([`e84148e`](https://github.com/bec-project/bec/commit/e84148ed3c2d8554a54d61de00ca55cbf2d606d9))

- Fixed bec client import for end2end tests
  ([`e896edb`](https://github.com/bec-project/bec/commit/e896edbd3b1d589ed0674c28f112bafeb90bc171))

- Fixed bug in config handler test
  ([`c62d84e`](https://github.com/bec-project/bec/commit/c62d84ea8ca5504f1e664cf2331f0f3c54fa4df9))

- Fixed bug in pipeline mock
  ([`d24de87`](https://github.com/bec-project/bec/commit/d24de874c3d31c4a778f7d0ad2124db47d3e6cc1))

- Fixed bug in scihub test
  ([`12f623c`](https://github.com/bec-project/bec/commit/12f623cda603fa8ab6802907a56e930293d74c23))

- Fixed fixture
  ([`bcd54ae`](https://github.com/bec-project/bec/commit/bcd54aede82713ff29f8668359763c5be87c5730))

- Fixed lamni tests
  ([`b5e3919`](https://github.com/bec-project/bec/commit/b5e391999fd8500ded6a50f40ae681a8e50d3aac))

- Fixed lock acquire
  ([`6a2a710`](https://github.com/bec-project/bec/commit/6a2a710273becdce1b39928dd54cefa93dc9fff2))

- Fixed messages for new rpc defaults
  ([`b841eca`](https://github.com/bec-project/bec/commit/b841ecaa64eb538a18d40866ff3dbf5b68bfa879))

- Fixed missing function after refactoring
  ([`8f8003b`](https://github.com/bec-project/bec/commit/8f8003baa16c13d2be26d5ce0de3546e9a19d0e3))

- Fixed missing import
  ([`2c9b60a`](https://github.com/bec-project/bec/commit/2c9b60a7da9dcbb3fab9b2384eabe01615abc50a))

- Fixed missing import
  ([`7c5c7f8`](https://github.com/bec-project/bec/commit/7c5c7f81875b2c99353f6e9ffef768ec94ca2683))

- Fixed sb test
  ([`0e56f95`](https://github.com/bec-project/bec/commit/0e56f956af4c214dd0c27df1807cc4d807272ed6))

- Fixed scihub tests
  ([`13e77ba`](https://github.com/bec-project/bec/commit/13e77ba2e31a63f7bd6a2f1ac7d9f68873e65f59))

- Fixed test
  ([`cdf75b8`](https://github.com/bec-project/bec/commit/cdf75b84803774d64bcb68c5f12532e22ee756f2))

- Fixed test for additional scan baseline
  ([`4f1b80e`](https://github.com/bec-project/bec/commit/4f1b80e8423bfebe1e882710d024373a59be9086))

- Fixed test for new data structure
  ([`a9837aa`](https://github.com/bec-project/bec/commit/a9837aa4fa446cd115685abe0c9e6425c8e96f70))

- Fixed test for new otf implementation
  ([`537203c`](https://github.com/bec-project/bec/commit/537203ce0c35f559c9ceeb90b162530ba7a9a461))

- Fixed test for refactored scan bundler
  ([`1466a07`](https://github.com/bec-project/bec/commit/1466a07fbd75b1a28f2bcbc4e7304cc3fc774961))

- Fixed test for updated rpc schema
  ([`b2284bc`](https://github.com/bec-project/bec/commit/b2284bcf7678aa8845be3337074d2f32940362ba))

- Fixed test_handle_device_instructions
  ([`3b86d3e`](https://github.com/bec-project/bec/commit/3b86d3e965198713694e091e8fea1256ef5c433d))

- Fixed tests
  ([`2cb847e`](https://github.com/bec-project/bec/commit/2cb847e3b16b076497599fb0901a790f74712043))

- Fixed tests
  ([`00c0c94`](https://github.com/bec-project/bec/commit/00c0c940c8a5609733d79c8d13fa68775298cc43))

- Fixed tests
  ([`79a02eb`](https://github.com/bec-project/bec/commit/79a02ebc35782fbb8ccc71f8997f2224537d5828))

- Fixed tests
  ([`8a1e761`](https://github.com/bec-project/bec/commit/8a1e7613a4bad1b1dfaead53b4ba368f1ce5a537))

- Fixed tests for acquisition priority
  ([`2d1d64c`](https://github.com/bec-project/bec/commit/2d1d64caf8d8b8379ab4ce247bdf8373c9034fb9))

- Fixed tests for left align bl_show_all; fixed lsamrot test
  ([`b9d4750`](https://github.com/bec-project/bec/commit/b9d47507ca31d9687522af0213050e5c1f345b3f))

- Fixed tests for min positions
  ([`c2578f6`](https://github.com/bec-project/bec/commit/c2578f648c03c85bd9f7abf78f8ea791e003862c))

- Fixed tests for new default exp time
  ([`d6b2c67`](https://github.com/bec-project/bec/commit/d6b2c67a9e2944e2a873945780fbdad39831b00b))

- Fixed tests for new kickoff interface
  ([`fb4fa18`](https://github.com/bec-project/bec/commit/fb4fa183213b699ff415668a3e386ab8120d2ee5))

- Fixed tests for new scihub integration
  ([`3424000`](https://github.com/bec-project/bec/commit/3424000efe5cf34df122f4e4789b23a97b177d4f))

- Fixed tests for new wait group structure
  ([`f0616ea`](https://github.com/bec-project/bec/commit/f0616ead21780b24edcfe5d85576ee79cd551299))

- Improved client tests
  ([`e387660`](https://github.com/bec-project/bec/commit/e3876609f22cb66a4933b494acf082c5c791590f))

- Improved get_devices_from_scan_data
  ([`0ff4b08`](https://github.com/bec-project/bec/commit/0ff4b08acfadd18a877da7ea77807039a7187bc1))

- Improved scan bundler tests
  ([`7bbf3a1`](https://github.com/bec-project/bec/commit/7bbf3a1c1ee4f130126f476b636beaf76aa9bec3))

- Improved tes_get_fermat_spiral_pos
  ([`31eec19`](https://github.com/bec-project/bec/commit/31eec194dc3387f658241e340e7958731bbbd0ba))

- Improved test_add_wait_group
  ([`984a5ac`](https://github.com/bec-project/bec/commit/984a5ac4eb91e0d6b074d6480aac2420c47bf2d8))

- Improved test_read_device
  ([`5953765`](https://github.com/bec-project/bec/commit/595376579b447803f8eed6fced56dc12bde7ec13))

- Improved tests
  ([`fd746cc`](https://github.com/bec-project/bec/commit/fd746cc8bf4137792a7a33aa5aaec2aef84f1ba0))

- Improved tests for scibec
  ([`cea26bb`](https://github.com/bec-project/bec/commit/cea26bb89797129dfea59f4e674fac060cb4fbfb))

- Minor cleanup
  ([`81f0a48`](https://github.com/bec-project/bec/commit/81f0a48e59ac2fa03053603088f042ce42802b9e))

- Moved test from utils to test dir
  ([`e13948d`](https://github.com/bec-project/bec/commit/e13948d2b0697a06d420c45a35f5b14b09cd0a59))

- Moved tests to bec lib
  ([`93a251f`](https://github.com/bec-project/bec/commit/93a251fc82109142a83229d221c943476735ce9a))

- Removed bec_client
  ([`0ca6d16`](https://github.com/bec-project/bec/commit/0ca6d1648374e47eaef4d0f340df31881facb297))

- Removed samx check
  ([`a11787e`](https://github.com/bec-project/bec/commit/a11787e052cb85e8fa2fd68fb40ccaa3126b6801))

- Removed test_get_scan_status, worked on my computer but not on pipeline, pipeline should be fixed
  now
  ([`00f9531`](https://github.com/bec-project/bec/commit/00f9531702d61ce75cb3332befc21d0a977d496a))

- Renamed test to avoid collision with the device_server
  ([`809f001`](https://github.com/bec-project/bec/commit/809f0010500f4578aafd4858cb9badf78bc5cf4f))

- Reverted changes in timeout tests
  ([`6f8358d`](https://github.com/bec-project/bec/commit/6f8358d7e6cfbbe05947ea5798f9232ab602e590))

- Test_redis_consumer_threaded_init
  ([`a64cc23`](https://github.com/bec-project/bec/commit/a64cc235b289ba22e07cecebada115748acbcb76))

- Test_stop, test_start
  ([`34571d2`](https://github.com/bec-project/bec/commit/34571d2d12ffa5e955685e068004e7fcc6b5235a))

- Test_update_device_metadata
  ([`e72d4e0`](https://github.com/bec-project/bec/commit/e72d4e07511fb174d49c2ad602f88d91d8aadaad))

- Test_update_status
  ([`b663290`](https://github.com/bec-project/bec/commit/b6632900393ec4f4e493ee434ff74df6381979c1))

- Trigger
  ([`f11ac5a`](https://github.com/bec-project/bec/commit/f11ac5aafb768ba837beed74c36b8c2d9e62e6f7))
