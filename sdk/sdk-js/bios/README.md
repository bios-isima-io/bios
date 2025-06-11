# biOS JS SDK

## Installation
### Create a Tunnel to Private NPM Registry
We use a private NPM registry to publish our bios JavaScript SDK. More information about this is at
https://isima.atlassian.net/wiki/spaces/BIOS/pages/1113260193

```
yarn add bios-sdk
```

## "Hello World" program

``` javascript
import bios from 'bios-sdk';

// The session is always global
bios.login({
  endpoint: 'https://bios-server-host',
  email: 'user@example.com',
  password: 'thepassword'
});

const signals = bios.getSignals();
```