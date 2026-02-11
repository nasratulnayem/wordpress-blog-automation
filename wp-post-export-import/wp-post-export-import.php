<?php
/**
 * Plugin Name: WP Post Export/Import (Media + Yoast)
 * Description: Export posts with featured images, media, tags, and Yoast meta; import into another site with media copied and URLs updated.
 * Version: 1.0.0
 * Author: GPL Mama
 */

if (!defined('ABSPATH')) {
    exit;
}

class WP_Post_Export_Import {
    const MENU_SLUG = 'wp-post-export-import';
    const EXPORT_ACTION = 'wppei_export';
    const IMPORT_ACTION = 'wppei_import';
    const ROLLBACK_ACTION = 'wppei_rollback';
    const PROGRESS_OPTION = 'wppei_progress';
    const IMPORT_SESSION_META = '_wppei_import_session';

    public function __construct() {
        add_action('admin_menu', [$this, 'register_menu']);
        add_action('admin_post_' . self::EXPORT_ACTION, [$this, 'handle_export']);
        add_action('admin_post_' . self::IMPORT_ACTION, [$this, 'handle_import']);
        add_action('admin_post_' . self::ROLLBACK_ACTION, [$this, 'handle_rollback']);
    }

    public function register_menu() {
        add_menu_page(
            'Post Export/Import',
            'Post Export/Import',
            'manage_options',
            self::MENU_SLUG,
            [$this, 'render_page'],
            'dashicons-migrate',
            80
        );
    }

    public function render_page() {
        if (!current_user_can('manage_options')) {
            return;
        }
        $export_url = admin_url('admin-post.php');
        $import_url = admin_url('admin-post.php');
        $rollback_url = admin_url('admin-post.php');
        $progress = get_option(self::PROGRESS_OPTION, []);
        $last_session = get_option('wppei_last_import_session', '');
        ?>
        <div class="wrap">
            <h1>Post Export/Import</h1>
            <p>Export posts with featured images, media files, tags, and Yoast SEO meta. Import into another site and reattach media with new URLs.</p>
            <?php if (!empty($progress)) : ?>
                <div class="notice notice-info">
                    <p>
                        <strong>Last run:</strong>
                        <?php echo esc_html($progress['stage'] ?? ''); ?>
                        <?php if (!empty($progress['message'])) : ?>
                            — <?php echo esc_html($progress['message']); ?>
                        <?php endif; ?>
                        <?php if (!empty($progress['updated_at'])) : ?>
                            (<?php echo esc_html($progress['updated_at']); ?>)
                        <?php endif; ?>
                    </p>
                </div>
            <?php endif; ?>

            <h2>Export</h2>
            <form method="post" action="<?php echo esc_url($export_url); ?>">
                <?php wp_nonce_field(self::EXPORT_ACTION); ?>
                <input type="hidden" name="action" value="<?php echo esc_attr(self::EXPORT_ACTION); ?>" />
                <p>
                    <label for="wppei_post_status">Post status</label>
                    <select name="post_status" id="wppei_post_status">
                        <option value="publish" selected>Publish</option>
                        <option value="any">Any</option>
                    </select>
                </p>
                <p>
                    <label for="wppei_post_type">Post type</label>
                    <input type="text" name="post_type" id="wppei_post_type" value="post" />
                </p>
                <?php submit_button('Export ZIP'); ?>
            </form>

            <hr />

            <h2>Import</h2>
            <form method="post" action="<?php echo esc_url($import_url); ?>" enctype="multipart/form-data">
                <?php wp_nonce_field(self::IMPORT_ACTION); ?>
                <input type="hidden" name="action" value="<?php echo esc_attr(self::IMPORT_ACTION); ?>" />
                <p>
                    <label for="wppei_zip">Export ZIP</label>
                    <input type="file" name="export_zip" id="wppei_zip" accept=".zip" required />
                </p>
                <p>
                    <label>
                        <input type="checkbox" name="replace_content_urls" value="1" checked />
                        Replace media URLs inside content
                    </label>
                </p>
                <?php submit_button('Import ZIP'); ?>
            </form>

            <hr />

            <h2>Rollback (Delete Last Import)</h2>
            <p>Use this if an import went wrong. It will delete all posts imported in the most recent session.</p>
            <?php if ($last_session) : ?>
                <form method="post" action="<?php echo esc_url($rollback_url); ?>">
                    <?php wp_nonce_field(self::ROLLBACK_ACTION); ?>
                    <input type="hidden" name="action" value="<?php echo esc_attr(self::ROLLBACK_ACTION); ?>" />
                    <input type="hidden" name="import_session" value="<?php echo esc_attr($last_session); ?>" />
                    <?php submit_button('Delete Last Imported Posts', 'delete'); ?>
                </form>
            <?php else : ?>
                <p><em>No import session found yet.</em></p>
            <?php endif; ?>
        </div>
        <?php
    }

    private function set_progress($stage, $current, $total, $message = '') {
        update_option(self::PROGRESS_OPTION, [
            'stage' => $stage,
            'current' => (int) $current,
            'total' => (int) $total,
            'message' => $message,
            'updated_at' => current_time('mysql'),
        ], false);
    }

    private function get_post_categories($post_id) {
        $terms = get_the_category($post_id);
        $cats = [];
        if (!empty($terms) && !is_wp_error($terms)) {
            foreach ($terms as $term) {
                $cats[] = [
                    'name' => $term->name,
                    'slug' => $term->slug,
                ];
            }
        }
        return $cats;
    }

    private function get_post_meta_export($post_id) {
        $meta = get_post_meta($post_id);
        if (empty($meta)) {
            return [];
        }
        $skip = [
            '_edit_lock',
            '_edit_last',
            '_wp_old_slug',
            '_wp_page_template',
            '_thumbnail_id',
            '_yoast_wpseo_title',
            '_yoast_wpseo_metadesc',
        ];
        $out = [];
        foreach ($meta as $key => $values) {
            if (in_array($key, $skip, true)) {
                continue;
            }
            $out[$key] = array_values($values);
        }
        return $out;
    }

    private function build_export_payload($post_type, $post_status) {
        $query_args = [
            'post_type' => $post_type,
            'post_status' => $post_status,
            'posts_per_page' => -1,
        ];
        $posts = get_posts($query_args);

        $this->set_progress('export', 0, count($posts), 'Starting export');
        $payload = [
            'version' => '1.0.0',
            'site' => home_url(),
            'generated_at' => current_time('mysql'),
            'post_type' => $post_type,
            'post_status' => $post_status,
            'posts' => [],
            'media' => [],
        ];

        $media_map = [];

        $index = 0;
        foreach ($posts as $post) {
            $index++;
            $post_id = $post->ID;
            $tags = wp_get_post_tags($post_id, ['fields' => 'names']);
            $yoast_title = get_post_meta($post_id, '_yoast_wpseo_title', true);
            $yoast_desc = get_post_meta($post_id, '_yoast_wpseo_metadesc', true);
            $categories = $this->get_post_categories($post_id);
            $custom_fields = $this->get_post_meta_export($post_id);

            $featured_id = get_post_thumbnail_id($post_id);
            $featured_url = $featured_id ? wp_get_attachment_url($featured_id) : '';

            $content = $post->post_content;

            // Collect attachments: featured + attachments attached to post + URLs in content
            $attachment_ids = [];
            if ($featured_id) {
                $attachment_ids[] = $featured_id;
            }
            $attached = get_children([
                'post_parent' => $post_id,
                'post_type' => 'attachment',
                'fields' => 'ids',
                'post_status' => 'inherit',
            ]);
            if (!empty($attached)) {
                $attachment_ids = array_merge($attachment_ids, $attached);
            }

            if (preg_match_all('/https?:\\/\\/[^"\'\s>]+/i', $content, $matches)) {
                foreach ($matches[0] as $url) {
                    $att_id = attachment_url_to_postid($url);
                    if ($att_id) {
                        $attachment_ids[] = $att_id;
                    }
                }
            }

            $attachment_ids = array_unique(array_filter($attachment_ids));

            foreach ($attachment_ids as $att_id) {
                if (isset($media_map[$att_id])) {
                    continue;
                }
                $file_path = get_attached_file($att_id);
                $file_url = wp_get_attachment_url($att_id);
                if (!$file_path || !file_exists($file_path)) {
                    continue;
                }
                $relative = ltrim(str_replace(wp_get_upload_dir()['basedir'], '', $file_path), '/');
                $media_map[$att_id] = [
                    'id' => $att_id,
                    'url' => $file_url,
                    'file' => $relative,
                ];
            }

            $payload['posts'][] = [
                'post_title' => $post->post_title,
                'post_name' => $post->post_name,
                'post_content' => $content,
                'post_excerpt' => $post->post_excerpt,
                'post_date' => $post->post_date,
                'post_modified' => $post->post_modified,
                'tags' => $tags,
                'categories' => $categories,
                'custom_fields' => $custom_fields,
                'yoast_title' => $yoast_title,
                'yoast_description' => $yoast_desc,
                'featured_image_url' => $featured_url,
            ];
            $this->set_progress('export', $index, count($posts), 'Exporting posts');
        }

        $payload['media'] = array_values($media_map);
        return $payload;
    }

    public function handle_export() {
        if (!current_user_can('manage_options')) {
            wp_die('Permission denied');
        }
        check_admin_referer(self::EXPORT_ACTION);

        $post_type = isset($_POST['post_type']) ? sanitize_text_field($_POST['post_type']) : 'post';
        $post_status = isset($_POST['post_status']) ? sanitize_text_field($_POST['post_status']) : 'publish';
        if ($post_status !== 'publish' && $post_status !== 'any') {
            $post_status = 'publish';
        }

        $payload = $this->build_export_payload($post_type, $post_status);
        $this->set_progress('export', 0, count($payload['media']), 'Packaging media');

        $tmp_dir = wp_upload_dir()['basedir'] . '/wppei-temp';
        if (!file_exists($tmp_dir)) {
            wp_mkdir_p($tmp_dir);
        }

        $json_path = tempnam($tmp_dir, 'wppei_');
        if (!$json_path) {
            wp_die('Failed to create temp file.');
        }

        file_put_contents($json_path, wp_json_encode($payload, JSON_UNESCAPED_SLASHES));

        $zip_path = tempnam($tmp_dir, 'wppei_export_');
        if (!$zip_path) {
            wp_die('Failed to create zip.');
        }

        $zip = new ZipArchive();
        if ($zip->open($zip_path, ZipArchive::OVERWRITE) !== true) {
            wp_die('Could not create ZIP archive.');
        }

        $zip->addFile($json_path, 'posts.json');

        $uploads_dir = wp_get_upload_dir()['basedir'];
        $media_index = 0;
        foreach ($payload['media'] as $media) {
            $media_index++;
            $abs_path = $uploads_dir . '/' . $media['file'];
            if (file_exists($abs_path)) {
                $zip->addFile($abs_path, 'media/' . $media['file']);
            }
            $this->set_progress('export', $media_index, count($payload['media']), 'Adding media to ZIP');
        }
        $zip->close();
        $this->set_progress('export', 1, 1, 'Export complete');

        @unlink($json_path);

        $filename = 'wp-post-export-' . date('Y-m-d-His') . '.zip';
        header('Content-Type: application/zip');
        header('Content-Disposition: attachment; filename=' . $filename);
        header('Content-Length: ' . filesize($zip_path));
        readfile($zip_path);
        @unlink($zip_path);
        exit;
    }

    private function copy_media_to_uploads($source_path, $relative_path) {
        $upload_dir = wp_get_upload_dir();
        $target_path = trailingslashit($upload_dir['basedir']) . ltrim($relative_path, '/');
        $target_dir = dirname($target_path);
        if (!file_exists($target_dir)) {
            wp_mkdir_p($target_dir);
        }
        if (!file_exists($target_path)) {
            copy($source_path, $target_path);
        }
        return $target_path;
    }

    private function insert_attachment_from_file($file_path, $relative_path, $old_url) {
        $upload_dir = wp_get_upload_dir();
        $filetype = wp_check_filetype(basename($file_path), null);
        $attachment = [
            'guid' => $upload_dir['baseurl'] . '/' . ltrim($relative_path, '/'),
            'post_mime_type' => $filetype['type'],
            'post_title' => sanitize_file_name(basename($file_path)),
            'post_content' => '',
            'post_status' => 'inherit',
        ];

        $attach_id = wp_insert_attachment($attachment, $file_path);
        require_once ABSPATH . 'wp-admin/includes/image.php';
        $attach_data = wp_generate_attachment_metadata($attach_id, $file_path);
        wp_update_attachment_metadata($attach_id, $attach_data);

        $new_url = wp_get_attachment_url($attach_id);
        return [
            'id' => $attach_id,
            'old_url' => $old_url,
            'new_url' => $new_url,
        ];
    }

    public function handle_import() {
        if (!current_user_can('manage_options')) {
            wp_die('Permission denied');
        }
        check_admin_referer(self::IMPORT_ACTION);

        if (empty($_FILES['export_zip']['tmp_name'])) {
            wp_die('No file uploaded.');
        }

        $replace_urls = !empty($_POST['replace_content_urls']);

        $tmp_file = $_FILES['export_zip']['tmp_name'];
        $zip = new ZipArchive();
        if ($zip->open($tmp_file) !== true) {
            wp_die('Invalid ZIP file.');
        }

        $extract_dir = wp_upload_dir()['basedir'] . '/wppei-import-' . time();
        wp_mkdir_p($extract_dir);
        $zip->extractTo($extract_dir);
        $zip->close();

        $json_path = $extract_dir . '/posts.json';
        if (!file_exists($json_path)) {
            wp_die('posts.json not found in ZIP.');
        }

        $payload = json_decode(file_get_contents($json_path), true);
        if (!is_array($payload) || empty($payload['posts'])) {
            wp_die('Invalid export file.');
        }

        $session_id = 'wppei_' . wp_generate_password(12, false, false) . '_' . time();
        update_option('wppei_last_import_session', $session_id, false);

        $url_map = [];
        if (!empty($payload['media'])) {
            $this->set_progress('import', 0, count($payload['media']), 'Importing media');
            $media_index = 0;
            foreach ($payload['media'] as $media) {
                $media_index++;
                $rel = isset($media['file']) ? $media['file'] : '';
                $old_url = isset($media['url']) ? $media['url'] : '';
                $source = $extract_dir . '/media/' . $rel;
                if (!$rel || !file_exists($source)) {
                    $this->set_progress('import', $media_index, count($payload['media']), 'Skipping missing media');
                    continue;
                }
                $dest_path = $this->copy_media_to_uploads($source, $rel);
                $inserted = $this->insert_attachment_from_file($dest_path, $rel, $old_url);
                if (!empty($inserted['old_url']) && !empty($inserted['new_url'])) {
                    $url_map[$inserted['old_url']] = $inserted['new_url'];
                }
                $this->set_progress('import', $media_index, count($payload['media']), 'Importing media');
            }
        }

        $this->set_progress('import', 0, count($payload['posts']), 'Importing posts');
        $post_index = 0;
        foreach ($payload['posts'] as $item) {
            $post_index++;
            $post_content = isset($item['post_content']) ? $item['post_content'] : '';
            if ($replace_urls && $url_map) {
                $post_content = str_replace(array_keys($url_map), array_values($url_map), $post_content);
            }

            $post_data = [
                'post_title' => isset($item['post_title']) ? $item['post_title'] : '',
                'post_name' => isset($item['post_name']) ? $item['post_name'] : '',
                'post_content' => $post_content,
                'post_excerpt' => isset($item['post_excerpt']) ? $item['post_excerpt'] : '',
                'post_status' => 'publish',
                'post_type' => 'post',
                'post_date' => isset($item['post_date']) ? $item['post_date'] : current_time('mysql'),
                'post_modified' => isset($item['post_modified']) ? $item['post_modified'] : current_time('mysql'),
            ];

            $new_post_id = wp_insert_post($post_data);
            if (is_wp_error($new_post_id)) {
                $this->set_progress('import', $post_index, count($payload['posts']), 'Post insert failed');
                continue;
            }
            update_post_meta($new_post_id, self::IMPORT_SESSION_META, $session_id);

            if (!empty($item['tags']) && is_array($item['tags'])) {
                wp_set_post_tags($new_post_id, $item['tags'], false);
            }

            if (!empty($item['categories']) && is_array($item['categories'])) {
                $cat_ids = [];
                foreach ($item['categories'] as $cat) {
                    $cat_name = isset($cat['name']) ? $cat['name'] : '';
                    $cat_slug = isset($cat['slug']) ? $cat['slug'] : '';
                    if (!$cat_name) {
                        continue;
                    }
                    $term = get_term_by('name', $cat_name, 'category');
                    if (!$term && $cat_slug) {
                        $term = get_term_by('slug', $cat_slug, 'category');
                    }
                    if (!$term) {
                        $created = wp_insert_term($cat_name, 'category', [
                            'slug' => $cat_slug ?: sanitize_title($cat_name),
                        ]);
                        if (!is_wp_error($created) && !empty($created['term_id'])) {
                            $cat_ids[] = (int) $created['term_id'];
                        }
                    } else {
                        $cat_ids[] = (int) $term->term_id;
                    }
                }
                if (!empty($cat_ids)) {
                    wp_set_post_categories($new_post_id, $cat_ids, false);
                }
            }

            if (!empty($item['yoast_title'])) {
                update_post_meta($new_post_id, '_yoast_wpseo_title', $item['yoast_title']);
            }
            if (!empty($item['yoast_description'])) {
                update_post_meta($new_post_id, '_yoast_wpseo_metadesc', $item['yoast_description']);
            }

            if (!empty($item['custom_fields']) && is_array($item['custom_fields'])) {
                foreach ($item['custom_fields'] as $meta_key => $values) {
                    if (!is_array($values)) {
                        $values = [$values];
                    }
                    foreach ($values as $value) {
                        add_post_meta($new_post_id, $meta_key, maybe_unserialize($value));
                    }
                }
            }

            if (!empty($item['featured_image_url']) && isset($url_map[$item['featured_image_url']])) {
                $feature_id = attachment_url_to_postid($url_map[$item['featured_image_url']]);
                if ($feature_id) {
                    set_post_thumbnail($new_post_id, $feature_id);
                }
            }
            $this->set_progress('import', $post_index, count($payload['posts']), 'Importing posts');
        }

        $this->delete_directory($extract_dir);
        $this->set_progress('import', 1, 1, 'Import complete');

        wp_redirect(admin_url('admin.php?page=' . self::MENU_SLUG . '&import=done'));
        exit;
    }

    public function handle_rollback() {
        if (!current_user_can('manage_options')) {
            wp_die('Permission denied');
        }
        check_admin_referer(self::ROLLBACK_ACTION);
        $session_id = isset($_POST['import_session']) ? sanitize_text_field($_POST['import_session']) : '';
        if (!$session_id) {
            wp_die('Missing import session.');
        }
        $this->set_progress('rollback', 0, 0, 'Deleting imported posts');
        $posts = get_posts([
            'post_type' => 'post',
            'post_status' => 'any',
            'posts_per_page' => -1,
            'meta_key' => self::IMPORT_SESSION_META,
            'meta_value' => $session_id,
        ]);
        $total = count($posts);
        $index = 0;
        foreach ($posts as $post) {
            $index++;
            wp_delete_post($post->ID, true);
            $this->set_progress('rollback', $index, $total, 'Deleting imported posts');
        }
        $this->set_progress('rollback', 1, 1, 'Rollback complete');
        wp_redirect(admin_url('admin.php?page=' . self::MENU_SLUG . '&rollback=done'));
        exit;
    }

    private function delete_directory($dir) {
        if (!is_dir($dir)) {
            return;
        }
        $items = scandir($dir);
        foreach ($items as $item) {
            if ($item === '.' || $item === '..') {
                continue;
            }
            $path = $dir . '/' . $item;
            if (is_dir($path)) {
                $this->delete_directory($path);
            } else {
                @unlink($path);
            }
        }
        @rmdir($dir);
    }
}

new WP_Post_Export_Import();
