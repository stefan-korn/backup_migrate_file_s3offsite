<?php
libraries_load('aws-sdk-php');
use Aws\S3\S3Client;
use Aws\S3\Exception\S3Exception;

/**
 * @file
 * A destination type for saving locally to the server and saving an offsite
 * copy to AWS S3 by using a cron based queue
 */

/**
 * A destination type for saving locally to the server and saving an offsite
 * copy to AWS S3 by using a cron based queue
 *
 * @ingroup backup_migrate_destinations
 */

class backup_migrate_destination_file_s3offsite extends backup_migrate_destination {
  var $supported_ops = array('scheduled backup', 'manual backup', 'local backup', 'restore', 'list files', 'configure', 'delete');

  function type_name() {
    return t("Server Directory with S3 offsite");
  }

  /**
   * Get the file location.
   */
  function get_realpath() {
    return drupal_realpath($this->get_location());
  }

  /**
   * File save destination callback.
   */
  function _save_file($file, $settings) {
    if ($this->confirm_destination() && $dir = $this->get_location()) {
      $filepath = rtrim($dir, "/") ."/". $file->filename();
      // Copy the file if there are multiple destinations.
      if (count($settings->get_destinations()) > 1) {
        $finalpath = file_unmanaged_copy($file->filepath(), $filepath);
      }
      // Otherwise we can move it and save a delete.
      else {
        $finalpath = file_unmanaged_move($file->filepath(), $filepath);
      }

      // chmod, chown and chgrp the file if needed.
      if ($chmod = $this->settings('chmod')) {
        if (!@drupal_chmod($filepath, octdec($chmod))) {
          _backup_migrate_message('Unable to set the file mode for: @file', array('@file' => $filepath), 'error');
        }
      }
      if ($chgrp = $this->settings('chgrp')) {
        if (!@chgrp($filepath, $chgrp)) {
          _backup_migrate_message('Unable to set the file group for: @file', array('@file' => $filepath), 'error');
        }
      }

      // AWS offsite transfer part
      $destinationid = reset($settings->destination_id);
      $destination = backup_migrate_get_destination($destinationid);
      $partNumber = 1;
      $queue = DrupalQueue::get('aws_offsite');
      $fileforq = fopen($finalpath, 'r');
      if($fileforq) {
        stream_set_chunk_size($fileforq, $destination->settings['chunksize'] * 1024 * 1024);
        $items = [];
        $s3 = s3_aws_offsite_init($destinationid);
        try {
        $response = $s3->createMultipartUpload([
          'Bucket' => $destination->settings['aws_bucket'],
          'Key' => basename($finalpath),
        ]);
        $uploadId = $response['UploadId'];
          watchdog(BACKUP_MIGRATE_S3OFFSITE_WATCHDOG,'Starting Multipart Upload to S3 succeeded. Key: %key, UploadId: %uploadid', array('%key' => basename($finalpath),'%uploadid' => $uploadId));
        } catch (S3Exception $e) {
          watchdog(BACKUP_MIGRATE_S3OFFSITE_WATCHDOG,'Starting Multipart Upload to S3 failed. Key: %key', array('%key' => basename($finalpath)),WATCHDOG_ERROR);
        }

        while (!feof($fileforq)) {
          $items[] = [
            'partno' => $partNumber,
            'filepath' => $finalpath,
            'filename' => basename($finalpath),
            'last' => FALSE,
            'uploadid' => $uploadId,
            'destination' => $destinationid,
            'chunksize' => $destination->settings['chunksize'],
            'Bucket' => $destination->settings['aws_bucket'],
          ];
          fread($fileforq, $destination->settings['chunksize'] * 1024 * 1024);
          $partNumber++;
        }
        fclose($fileforq);
        $numitems = count($items);
        $i = 0;
        foreach ($items as $key => $item) {
          if (++$i == $numitems) {
            $item['last'] = TRUE;
          }
          $queue->createItem($item);
        }
      }
      else {
        watchdog(BACKUP_MIGRATE_S3OFFSITE_WATCHDOG,'Error starting AWS offsite transfer. Opening file %filepath failed.', array('%filepath' => $finalpath),WATCHDOG_ERROR);
      }
      // End AWS Offsite transfer part

      return $file;
    }
  }

  /**
   * Determine if we can read the given file.
   */
  function can_read_file($file_id) {
    return $this->op('restore') && is_readable($this->get_filepath($file_id));
  }

  /**
   * File load destination callback.
   */
  function load_file($file_id) {
    $filepath = $this->get_filepath($file_id);
    if (file_exists($filepath)) {
      backup_migrate_include('files');
      return new backup_file(array('filepath' => $filepath));
    }
  }

  /**
   * Get the file object for the given file.
   */
  function get_file($file_id) {
    $files = $this->list_files();
    if (isset($files[$file_id])) {
      isset($files[$file_id]);
    }
    return NULL;
  }

  /**
   * File list destination callback.
   */
  function _list_files() {
    $files = array();
    if ($dir = $this->get_realpath()) {
      if ($handle = @opendir($dir)) {
        backup_migrate_include('files');
        while (FALSE !== ($file = readdir($handle))) {
          if (substr($file, 0, 1) !== '.') {
            $filepath = $dir ."/". $file;
            $files[$file] = new backup_file(array('filepath' => $filepath));          
          }
        }
      }
    }
    return $files;
  }

  /**
   * File delete destination callback.
   */
  function _delete_file($file_id) {
    $filepath = $this->get_filepath($file_id);
    file_unmanaged_delete($filepath);
  }

  /**
   * Get the filepath from the given file id.
   */
  function get_filepath($file_id) {
    if ($dir = $this->get_realpath()) {
      $filepath = rtrim($dir, '/') .'/'. $file_id;
      return $filepath;
    }
    return FALSE;
  }

  /**
   * Get the form for the settings for the files destination.
   */
  function edit_form() {
    $form = parent::edit_form();
    $form['location'] = array(
      "#type" => "textfield",
      "#title" => t("Directory path"),
      "#default_value" => $this->get_location(),
      "#required" => TRUE,
      "#description" => t('Enter the path to the directory to save the backups to. Use a relative path to pick a path relative to your Drupal root directory. The web server must be able to write to this path.'),
    );
    $form['aws_host'] = array(
      '#type' => 'textfield',
      '#title' => t('AWS Host'),
      '#default_value' => $this->settings?$this->settings['aws_host']:'s3.amazonaws.com',
      '#required' => TRUE,
      '#description' => t('Enter the S3 AWS host, without the scheme part, will be https:// prefixed automatically.')
    );
    $form['aws_bucket'] = array(
      '#type' => 'textfield',
      '#title' => t('AWS bucket name'),
      '#required' => TRUE,
      '#description' => t('Enter the AWS S3 bucket name, must exist already.'),
      '#default_value' => $this->settings?$this->settings['aws_bucket']:'',
    );
    $form['aws_accesskey'] = array(
      '#type' => 'textfield',
      '#title' => t('AWS Access Key'),
      '#required' => TRUE,
      '#description' => t('Enter the AWS access key.'),
      '#default_value' => $this->settings?$this->settings['aws_accesskey']:'',
    );
    $form['aws_secret_accesskey'] = array(
      '#type' => 'password',
      '#title' => t('AWS secret access key'),
      '#description' => t('Enter the AWS secret access key.'),
      '#element_validate' => array('_bmfs3_seckey_validate'),
    );
    $form['s3_region'] = array(
      '#type' => 'textfield',
      '#title' => t('AWS S3 region'),
      '#description' => t('Optional specify region if necessary, system tries to evaluate the region.'),
      '#default_value' => $this->settings?$this->settings['s3_region']:'',
    );
    $form['cron_workertime'] = array(
      '#type' => 'textfield',
      '#title' => t('Cron worker time (seconds)'),
      '#description' => t('Enter the time that cron will use for uploading files to S3.
       After that time cron will stop uploading and start again in next cron.
       This is as site wide setting.'),
      '#default_value' => variable_get('backup_migrate_s3offsite_workertime'),
      '#element_validate' => array('_bmfs3_numeric_validate'),
    );
    $form['chunksize'] = array(
      '#type' => 'textfield',
      '#title' => t('Upload chunk size (MB)'),
      '#description' => t('Enter the upload chunk size. AWS multipart upload
      will use this size and split larger files in chunks of that size. You should select
      a chunk size that can be handled at least once in worker time.'),
      '#default_value' => $this->settings?$this->settings['chunksize']:'10',
      '#element_validate' => array('_bmfs3_numeric_validate'),
    );
    $form['settings'] = array(
      '#type' => 'fieldset',
      '#title' => t('Advanced Settings'),
      '#tree' => TRUE,
      '#collapsible' => TRUE,
      '#collapsed' => TRUE,
    );
    if (function_exists('chmod')) {
      $form['settings']['chmod'] = array(
        '#type' => 'textfield',
        '#title' => t('Change file mode (chmod)'),
        '#size' => 5,
        '#default_value' => $this->settings('chmod'),
        '#description' => t('If you enter a value here, backup files will be chmoded with the mode you specify. Specify the mode in octal form (e.g. 644 or 0644) or leave blank to disable this feature.'),
      );
    }
    if (function_exists('chgrp')) {
      $form['settings']['chgrp'] = array(
        '#type' => 'textfield',
        '#title' => t('Change file group (chgrp)'),
        '#size' => 5,
        '#default_value' => $this->settings('chgrp'),
        '#description' => t('If you enter a value here, backup files will be chgrped to the group you specify. Leave blank to disable this feature.'),
      );
    }
    return $form;
  }

  /**
   * Validate the form for the settings for the files destination.
   */
  function edit_form_validate($form, &$form_state) {
    $values = $form_state['values'];
    if (isset($values['settings']['chmod']) && !empty($values['settings']['chmod']) && !preg_match('/0?[0-7]{3}/', $values['settings']['chmod'])) {
      form_set_error('chmod', t('You must enter a valid chmod octal value (e.g. 644 or 0644) in the change mode field, or leave it blank.'));
    }
    parent::edit_form_validate($form, $form_state);

    if (!class_exists('Aws\S3\S3Client')) {
      $requirements = backup_migrate_file_s3offsite_requirements('runtime');
      $e_msg = t('Library not found or cannot be loaded.');
      if ($requirements['backup_migrate_file_s3offsite']['severity'] == REQUIREMENT_OK) {
        $e_msg .= ' ' . t('You might have to <a href="!cache">clear cache</a> if you installed the library after enabling this module.', array('!cache' => url('admin/config/development/performance')));
      }
      else {
        $e_msg .= ' ' . $requirements['backup_migrate_file_s3offsite']['description'];
      }
      form_set_error('', $e_msg);
    }
    else {
      // Do not attempt validation if there are errors in the form.
      if (form_get_errors()) return;

      $this->validate_s3_connection($form_state);
    }
  }
  /*
   * Validating the S3 connection during destination setup
   */
  function validate_s3_connection(&$form_state) {
    try {
      $secret = $form_state['values']['aws_secret_accesskey'];
      if(empty($form_state['values']['aws_secret_accesskey'])) {
        if (is_object($form_state['values']['item']) && property_exists($form_state['values']['item'], 'settings')) {
          if (!empty($form_state['values']['item']->settings['aws_secret_accesskey'])) {
            $secret = $form_state['values']['item']->settings['aws_secret_accesskey'];
          }
        }
      }
      $config = array(
        'base_url' => 'https://' . $form_state['values']['aws_host'],
        'key'      =>$form_state['values']['aws_accesskey'],
        'secret'   => $secret,
        'signature' => 'v4',
        'region' => $form_state['values']['s3_region'],
      );

      $this->s3 = S3Client::factory($config);

      $o_iter = $this->s3->getIterator('ListObjects', array(
        'Bucket' => $form_state['values']['aws_bucket'],
      ));
      foreach ($o_iter as $o) {
      }
    }
    catch (Exception $e) {
      $e_msg = 'There was an S3 error attempting to validate the settings below - %code %error';
      $e_args = array('%error' => $e->getMessage(), '%code' => $e->getCode());
      if(preg_match('~the region (.+) is wrong~', $e_args['%error'])) {

        preg_match('~expecting \'(.+)\'~', $e_args['%error'],$output);
        $form_state['values']['s3_region'] = $output[1];
        $this->validate_s3_connection($form_state);
      }
      else {
        if (method_exists($e, 'getAwsErrorCode')) {
          $e_args['%code'] = $e->getAwsErrorCode();
        }
        form_set_error('', t($e_msg, $e_args));
      }
    }
  }

  /**
   * Submit the form for the settings for the files destination.
   */
  function edit_form_submit($form, &$form_state) {
    // Add a 0 to the start of a 3 digit file mode to make it proper PHP encoded octal.
    if (strlen($form_state['values']['settings']['chmod']) == 3) {
      $form_state['values']['settings']['chmod'] = '0' . $form_state['values']['settings']['chmod'];
    }
    $form_state['values']['settings']['aws_host'] = $form_state['values']['aws_host'];
    $form_state['values']['settings']['aws_bucket'] = $form_state['values']['aws_bucket'];
    $form_state['values']['settings']['aws_accesskey'] = $form_state['values']['aws_accesskey'];
    if(empty($form_state['values']['aws_secret_accesskey'])) {
      if (is_object($form_state['values']['item']) && property_exists($form_state['values']['item'], 'settings')) {
        if (!empty($form_state['values']['item']->settings['aws_secret_accesskey'])) {
          $form_state['values']['settings']['aws_secret_accesskey'] = $form_state['values']['item']->settings['aws_secret_accesskey'];
        }
      }
    }
    else {
      $form_state['values']['settings']['aws_secret_accesskey'] = $form_state['values']['aws_secret_accesskey'];
    }
    $form_state['values']['settings']['s3_region'] = $form_state['values']['s3_region'];
    variable_set('backup_migrate_s3offsite_workertime', $form_state['values']['cron_workertime']);
    $form_state['values']['settings']['cron_workertime'] = $form_state['values']['cron_workertime'];
    $form_state['values']['settings']['chunksize'] = $form_state['values']['chunksize'];
    parent::edit_form_submit($form, $form_state);
  }

  /**
   * Check that a destination is valid.
   */
  function confirm_destination() {
    if ($dir = $this->get_location()) {
      return $this->check_dir($dir);
    }
    return FALSE;
  }

  /**
   * Prepare the destination directory for the backups.
   */
  function check_dir($directory) {
    if (!file_prepare_directory($directory, FILE_CREATE_DIRECTORY)) {
      // Unable to create destination directory.
      _backup_migrate_message("Unable to create or write to the save directory '%directory'. Please check the file permissions of that directory and try again.", array('%directory' => $directory), "error");
      return FALSE;
    }

    // If the destination directory is within the webroot, then secure it as best we can.
    if ($this->dir_in_webroot($directory)) {
      $directory = $this->check_web_dir($directory);
    }

    return $directory;
  }

  /**
   * Check that a web accessible directory has been properly secured, otherwise attempt to secure it.
   */
  function check_web_dir($directory) {
    // Check if the file has already been tested.
    if (is_file($directory .'/tested.txt')) {
      return $directory;
    }
    else {
      file_create_htaccess($directory, TRUE);
  
      // Check the user agent to make sure we're not responding to a request from drupal itself.
      // That should prevent infinite loops which could be caused by poormanscron in some circumstances.
      if (strpos($_SERVER['HTTP_USER_AGENT'], 'Drupal') !== FALSE) {
        return FALSE;
      }
  
      // Check to see if the destination is publicly accessible
      $test_contents = "this file should not be publicly accessible";
      // Create the the text.txt file if it's not already there.
      if (!is_file($directory .'/test.txt') || file_get_contents($directory .'/test.txt') != $test_contents) {
        if ($fp = fopen($directory .'/test.txt', 'w')) {
          @fputs($fp, $test_contents);
          fclose($fp);
        }
        else {
          $message = t("Security notice: Backup and Migrate was unable to write a test text file to the destination directory %directory, and is therefore unable to check the security of the backup destination. Backups to the server will be disabled until the destination becomes writable and secure.", array('%directory' => $directory));
          drupal_set_message($message, "error");
          return FALSE;
        }
      }
  
      // Attempt to read the test file via http. This may fail for other reasons,
      // so it's not a bullet-proof check.
      if ($this->test_file_readable_remotely($directory .'/test.txt', $test_contents)) {
        $message = t("Security notice: Backup and Migrate will not save backup files to the server because the destination directory is publicly accessible. If you want to save files to the server, please secure the '%directory' directory", array('%directory' => $directory));
        drupal_set_message($message, "error");
        return FALSE;
      }
      // Directory tested OK, so we mark it as tested.
      if ($fp = fopen($directory .'/tested.txt', 'w')) {
        $contents = t('The presence of this file indicates that this directory has been tested as safe to use as a destination for Backup and Migrate. If you change the permissions of this directory or change your web server settings, please delete this file so that the directory can be checked again.');
        @fputs($fp, $contents);
        fclose($fp);
      }
      return $directory;
    }
  }

  /**
   * Check if the given directory is within the webroot and is therefore web accessible.
   */
  function dir_in_webroot($directory) {
    $real_dir = drupal_realpath($directory);
    $real_root = drupal_realpath(DRUPAL_ROOT);
    if ($real_dir == $real_root || strpos($real_dir, $real_root . '/') === 0) {
      return TRUE;
    }
    return FALSE;
  }

/**
   * Check if a file can be read remotely via http.
   */
  function test_file_readable_remotely($directory, $contents) {
    $real_dir = drupal_realpath($directory);
    $real_root = drupal_realpath(DRUPAL_ROOT);
    if ($real_dir && $real_root) {
      // Get the root relative path.
      $path = substr($real_dir, strlen($real_root));

      $url = $GLOBALS['base_url'] . str_replace('\\', '/', $path);
      $result = drupal_http_request($url);
      if (!empty($result->data) && strpos($result->data, $contents) !== FALSE) {
        return TRUE;
      }
    }
    return FALSE;
  }

}

