//
//  TQTaskPriority.swift
//  AsyncTaskQueue
//
//  Created by joca on 7/7/19.
//  Copyright © 2019 joca. All rights reserved.
//

import Foundation

public enum TQTaskPriority: TimeInterval {
	case veryLow = 1800
	case low = 600
	case normal = 0
	case high = -600
	case veryHigh = -1800
}
