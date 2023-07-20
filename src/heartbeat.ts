// ATTENTION: Please do not use this. I'm keeping it here for posterity, but it
// will introduce a lot of concurrency patterns that I don't want in the codebase.
export default function heartbeat(cb: () => Promise<void>, ms: number) {
	let currentTimeout: NodeJS.Timeout | null = null
	const resume = () => {
		if (currentTimeout) {
			clearTimeout(currentTimeout)
		}
		currentTimeout = setTimeout(async () => {
			await cb()
			resume()
		}, ms)
	}

	const pause = () => {
		if (currentTimeout) {
			clearTimeout(currentTimeout)
		}
	}

	resume()

	return {
		resume,
		pause,
	}
}
