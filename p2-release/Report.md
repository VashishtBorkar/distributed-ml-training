Vashisht Borkar (vb471) Saransh Batwe (ssb209)

## Performance 
Average of 2,748 steps/sec over 5 runs, ranging from 2,676 to 2,812

The clerk caches its connection to the primary and the primary caches its connection to the backup. This saves having to open a connection for every write. The primary waits for the backup to acknowledge before it returns success. 

## AI Disclosure
### AI Tool
ChatGPT

### Parts of the project used for
General go syntax and libraries 

### Prompts 
how to generate unique hash id in Go?

Explain net and rpc library in go?

