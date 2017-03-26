# -*- coding: utf-8 -*-
from urllib import unquote_plus
from PIL import Image
import os
import time
import boto3


TMP = "/tmp"
PICTURE_DIR = TMP + "/pictures"
PIC_QUALITY = 50
THUMB_SIZE = (128, 128)

s3 = boto3.client("s3")
print("Loading function")


def handler(event, context):
    """
        this handler provides a image conversion library accept the following
        formats: jpg, jpe, jpg, png, bpm, tiff and converts them in JPEG with
        default quality set to 50%.
    """

    message = {"error": None}

    bucket, key, fin, fout = get_info_from(event)
    ret = exec_wrap(s3.download_file)(message, bucket, key, fin)
    if ret["error"] != None:
        return ret

    print("Starting image conversion")
    t1 = time.time()
    try:
        typ, _ = get_image_type(fin)
        if typ == "JPEG" or "TIFF" or "PNG" or "BMP":
            ori = Image.open(fin)
            ori.thumbnail(THUMB_SIZE)
            ori.save(fout, "JPEG", quality=PIC_QUALITY)
        else:
            message["error"] = "Image type {} not supported.".format(typ)
            return message
    except Exception as e:
        message["error"] = e
        return message
    t2 = time.time()
    print("Performed image conversion in: {:.3f}s.".format(t2 - t1))

    ret = exec_wrap(s3.upload_file)(message, bucket, fout[5:], None, fout)
    if ret["error"] != None:
        return ret

    return message


def get_image_type(name):
    try:
        typ = Image.open(name).format
        return typ, None
    except Exception as e:
        return "", e


def get_info_from(event):
    bucket = event['Records'][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    pic_name = key.split("/")[1]
    fin = "{}/{}".format(TMP, pic_name)
    fout = "{}/{}".format(PICTURE_DIR, pic_name.split(".")[0]+".thumbnails")

    return bucket, key, fin, fout


def exec_wrap(func):
    func_name = func.func_name.replace("_", " ")

    def func_wrapper(msg, bucket, key, fin=None, fout=None):
        print("Starting to {}.".format(func_name))
        t1 = time.time()
        try:
            if not os.path.exists(PICTURE_DIR):
                os.mkdir(PICTURE_DIR)
                os.chmod(PICTURE_DIR, 0o777)
            if func.im_func.func_name == "upload_file":
                func(fout, bucket, key)
                os.remove(fout)
            elif func.im_func.func_name == "download_file":
                func(bucket, key, fin)
            elif func.im_func.func_name == "delete_object":
                func(Bucket=bucket, Key=key)
        except Exception as e:
            msg["error"] = e
        t2 = time.time()
        print("Performed {} in: {:.2f}s.".format(func_name, t2 - t1))

        return msg
    return func_wrapper
