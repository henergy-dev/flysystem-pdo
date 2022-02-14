<?php

namespace Phlib\Flysystem\Pdo;

class Util
{
    /**
     * Get normalized pathinfo
     *
     * @param   string  $path
     * @return  array   pathinfo
     */
    public static function pathinfo($path)
    {
        $pathinfo = pathinfo($path) + compact('path');
        $pathinfo['dirname'] = static::normalizeDirname($pathinfo['dirname']);

        return $pathinfo;
    }

    public static function normalizeDirname($dirname)
    {
        if ($dirname === '.') {
            return '';
        }

        return $dirname;
    }

    /**
     * Emulate directies
     *
     * @param   array  $listing
     * @return  array  listing with emulated directories
     */
    public static function emulateDirectories(array $listing)
    {
        $directories = array();

        foreach ($listing as $object) {
            if ( ! empty($object['dirname']))
                $directories[] = $object['dirname'];
        }

        $directories = array_unique($directories);

        foreach ($directories as $directory) {
            $listing[] = static::pathinfo($directory);
        }

        return $listing;
    }
}
