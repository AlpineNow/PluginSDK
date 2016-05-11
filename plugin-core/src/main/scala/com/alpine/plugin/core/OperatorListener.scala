/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core

import com.alpine.plugin.core.annotation.AlpineSdkApi

/**
 * A listener can be used to pass messages from the plugin operators to the
 * UI console.
 */
@AlpineSdkApi
trait OperatorListener {
    /**
     * Will print a message to the 'results status' bar in the UI.
     * @param msg The message to be printed.
     */
    def notifyMessage(msg: String): Unit

    /**
     * Will print a message in red text to the 'results status' bar in the UI.
     * @param error The message to be printed.
     */
    def notifyError(error: String): Unit

}
