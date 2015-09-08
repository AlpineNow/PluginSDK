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
     * @param msg
     */
    def notifyMessage(msg: String): Unit

    /**
     * Will  message in red text starting with 'Error' to the 'results status' bar in the UI.
     * @param error
     */
    def notifyError(error: String): Unit

    /**
     * This will create a progress bar on the UI console if one with the
     * matching bar Id doesn't exist.
     * If the bar Id already exists, it'll update the progress to the given
     * value.
     * The progress value should be between 0 and 1.
     * @param progressBarId The progress bar Id (also the display name).
     * @param currentProgress A floating number between 0,0 and 1.0.
     */
    def notifyProgress(
        progressBarId: String,
        currentProgress: Float
    ): Unit
}
