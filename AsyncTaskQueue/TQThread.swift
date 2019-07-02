//
//  TQThread.swift
//  AsynchTaskQueue
//
//  Created by joca on 6/30/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

internal class TQThread: Thread {

	internal override func main() {
		while true {
			var shouldBreak = false

			autoreleasepool {
				if let task = TQQueue.shared.getNextTask() {
					task.run()
				} else {
					shouldBreak = true
				}
			}

			if shouldBreak {
				break
			}
		}
	}

}
