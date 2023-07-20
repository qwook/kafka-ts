/**
 * This is just to help with hot module reload development. Not sure if I even needed
 * to pull this out into a module to be honest.
 */

let loopedFunction: undefined | (() => Promise<void>)
let currentImmediate: NodeJS.Immediate | undefined

export async function looper() {
	let storedLoopedFunction = loopedFunction
	try {
		if (loopedFunction) await loopedFunction()
	} catch (e) {
		console.error(e)
	}
    // If the looped function has changed, we don't want to keep looping.
	if (storedLoopedFunction === loopedFunction) {
		currentImmediate = setImmediate(looper)
	}
}

export function setLoopedFunction(fn: () => Promise<void>) {
	if (currentImmediate) clearImmediate(currentImmediate)
	currentImmediate = undefined
	loopedFunction = fn
	looper()
}
