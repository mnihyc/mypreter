### [Under Development / DEMO Only]

I'm currently lacking time to complete the remaining parts / optimization. This project only acts as a DEMO as of now. 

I will continue contributing to it in the next few months.

### MyPreter

A custom C&C software that natively supports **DNS** tunnel, also including TCP, UDP, ......

### How to build

Newest **Rust** equipped; Then simply run **build.sh**

### Development

Current demo only has none encryption and TCP stream. However, the primitive design allows the possible routing via DNS tunnel by a custom underlying protocol (Request-ACK mode) like how TCP handles packets. Maximum 1.5RTT handshake of noise protocol is expected to be utilized too.

See detail in https://github.com/mnihyc/mypreter/blob/main/proto/src/proto.rs

The final version should depict a fast, reliable and secure C&C software supporting various protocols and encryptions.