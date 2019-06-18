Idea in meta language

```
for {
	scripts <- loadScripts(path)
	lastScript <- getLastApplied
	notYetApplied <- verifyScriptsChecksum(scripts, lastScript)
	applied <- applyScripts(notYetApplied)
} use result 

match applied {
	n script run successfully
	nothing to apply
	checksum error on scripts [x, y, ...]
}
```