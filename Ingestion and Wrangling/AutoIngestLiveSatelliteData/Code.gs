/*
|--------------------------------------------------------------------------
| Push live NASA data to S3 storage 
|--------------------------------------------------------------------------
|Uses a REST API as an Amazon S3 proxy to upload: API created in API Gateway
|By Christopher O'Brien
|
| Acknowledgements: https://gist.github.com/benbjurstrom/00cdfdb24e39c59c124e812d5effa39a
|
*/

// Purge messages automatically after how many days?
var CYCLE_DAYS = 1

// Maximum number of message threads to process per run. 
var PAGE_SIZE = 20

/**
 * Create a trigger that executes the purge function every day.
 * Execute this function to install the script.
 */
function setPurgeTrigger() {
  ScriptApp
    .newTrigger('attach')
    .timeBased()
    .everyDays(1)
    .create()
}

/**
 * Deletes all of the project's triggers
 * Execute this function to unintstall the script.
 */
function removeAllTriggers() {
  var triggers = ScriptApp.getProjectTriggers()
  for (var i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i])
  }
}

/**
 * Uploads email attachments from the inbox that were sent within the last day
 * and that are related to MODIS-c6
 */
function attach() {
  // search inbox for relative email data
  var search = 'in:inbox Subject:FIRMS Rapid Alert Id 41318, modis-c6 (Gtown Wildfire alert)  has:attachment newer_than:' + CYCLE_DAYS + 'd';
  //parse emails in threads, parse attachments in emails   
  var threads = GmailApp.search(search, 0, PAGE_SIZE);
  var msgs = GmailApp.getMessagesForThreads(threads);
  for (var i = 0 ; i < msgs.length; i++) {
    for (var j = 0; j < msgs[i].length; j++) {
      var attachments = msgs[i][j].getAttachments();
      //Send attachments to S3 storage 
      for (var k = 0; k < attachments.length; k++) {
        var atta_blob = attachments[k];
        var formData = {
                          'body': atta_blob
                        };

        var options = {
                        'method' : 'put',
                        'payload' : formData
                      };
                      //Removed full API link due to securuty concerns
                      UrlFetchApp.fetch('https://1zb*******.execute-api.us-east-1.amazonaws.com/dev/firmsauto/'+ attachments[k].getName(), options);


        Logger.log('Message "%s" contains the attachment "%s" (%s bytes)',
                  msgs[i][j].getSubject(), attachments[k].getName(), attachments[k].getSize());
      }
    }
  }  
}
