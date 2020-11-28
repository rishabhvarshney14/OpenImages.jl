module OpenImages
using HTTP
using CSV
using DataFrames
using AWSS3
using AWSCore

OID_v4 = "https://storage.googleapis.com/openimages/2018_04/"
OID_v5 = "https://storage.googleapis.com/openimages/v5/"

"""
    class_label_codes(class_labels, csv_dir=nothing)
Gets a dictionary that maps a list of OpenImages image class labels to their
corresponding image class label codes.

# Arguments
- `class_labels`: list of labels for OpenImages dataset to download.
- `csv_dir`: directory where we should look for the class annotation CSV file.
"""
function class_label_codes(class_labels, csv_dir=nothing)
    classes_csv = "class-descriptions-boxable.csv"
    url = OID_v5 * classes_csv
    response = HTTP.get(url, redirect=true)
        
    if response.status != 200
        println("Error!")
    end

    df_classes = CSV.File(response.body, header=false)

    if csv_dir === nothing
        csv_dir = pwd()
    end
    descriptions_csv_file_path = joinpath(csv_dir, classes_csv)

    df_classes = CSV.File(response.body, header=false)
    CSV.write(descriptions_csv_file_path, df_classes)

    labels_to_codes = Dict()
    for class_label in df_classes
        if class_label.Column2 in class_labels
            labels_to_codes[class_label.Column2] = class_label.Column1
        end
    end
    return labels_to_codes
end

"""
    get_annotations_csv(section)
Requests the annotations CSV for a split section.

# Arguments
- `section`: the relevant split section, "train", "validation", or "test".
"""
function get_annotations_csv(section)
    url = OID_v5 * section * "-annotations-bbox.csv"
    response = HTTP.get(url, redirect=true)
        
    if response.status != 200
        println("Error!")
    end

    return response
end

"""
    group_bounding_boxes(section, label_codes, csv_dir=nothing)
Returns a dictionary with image label as key and GroupedDataFrame as the value.

# Arguments
- `section`: the relevant split section, "train", "validation", or "test".
- `label_codes`: dictionary with class labels mapped to the image class.
- `csv_dir`: directory where we should look for the class annotation CSV file.
"""
function group_bounding_boxes(section, label_codes, csv_dir=nothing)
    if csv_dir === nothing
        response = get_annotations_csv(section)
        df_images = CSV.File(response.body)
    else
        bbox_csv_file_path = joinpath(csv_dir, section * "-annotations-bbox.csv")
        if !isfile(bbox_csv_file_path)
            response = get_annotations_csv(section)
            body = CSV.File(response.body)
            CSV.write(bbox_csv_file_path, body)
        end
        df_images = CSV.File(bbox_csv_file_path) |> DataFrame
    end

    unnecessary_columns = [
        "IsOccluded",
        "IsTruncated",
        "IsGroupOf",
        "IsDepiction",
        "IsInside",
        "Source",
        "Confidence",
    ]

    deletecols!(df_images, unnecessary_columns)

    labels_to_bounding_box_groups = Dict()
    for (labels, codes) in label_codes
        df_label_images = df_images[df_images.LabelName .== codes,:]
        deletecols!(df_label_images, ["LabelName"])
        labels_to_bounding_box_groups[labels] = groupby(df_label_images, "ImageID")
    end
    return labels_to_bounding_box_groups
end

"""
    download_single_image(image_file_path, dest_file_path)
Downloads and saves an image file from the OpenImages dataset.
"""
function download_single_image(image_file_path, dest_file_path)
    try
        config = AWSCore.aws_config(creds=nothing)
        AWSS3.s3_get_file(config, "open-images-dataset", image_file_path, dest_file_path)
    catch
        print("Error!")
    end
end

"""
    download_images_by_id(image_ids, section, image_directory)
Downloads a collection of images from OpenImages dataset.
"""
function download_images_by_id(image_ids, section, image_directory)
    for image_id in image_ids
        image_file_name = image_id * ".jpg"
        image_file_path = section * "/" * image_file_name
        dest_file_path = joinpath(image_directory, image_file_name)
        download_single_image(image_file_path, dest_file_path)
    end
end

"""
    download_images(dest_dir, class_labels, csv_dir=nothing, limit=nothing)
Downloads a dataset of images for a specified list of OpenImages image classes and returns
dictionary of the images directory for each class label.

# Arguments
- `dest_dir`: base directory under which the images and annotations will be stored.
- `class_labels`: list of labels for OpenImages dataset to download.
- `csv_dir`: directory where we should look for required CSV file (if not present files will be downloaded).
- `limit`: the maximum number of images per label to be download.
"""
function download_images(dest_dir, class_labels, csv_dir=nothing, limit=nothing)
    if csv_dir !== nothing && !isdir(csv_dir)
        mkdir(csv_dir)
    end

    labels_to_codes = class_label_codes(class_labels, csv_dir)

    class_directories = Dict()
    for (label, code) in labels_to_codes
        images_dir = joinpath(dest_dir, label, "images")
        if !isdir(images_dir)
            mkdir(images_dir)
        end

        class_directories[label] = images_dir
    end

    class_labels = [label for (label, value) in labels_to_codes]
    label_download_counts = Dict()
    for labels in class_labels
        label_download_counts[labels] = 0
    end

    for section in ("train", "validation", "test")
        label_bbox_groups = group_bounding_boxes(section, label_to_codes, csv_dir)

        for (label_index, class_label) in enumerate(class_labels)
            bbox_groups = label_bbox_groups[class_label]
            image_ids = groupvars(bbox_groups)

            if limit !== nothing
                remaining = limit - label_download_counts[class_label]
                if remaining <= 0
                    break
                elseif remaining <= length(image_ids)
                    image_ids = image_ids[1:remaining]
                end
            end
            download_images_by_id(image_ids, section, class_directories[class_label])
            label_download_counts[class_label] += length(image_ids)
        end
    end
    return class_directories
end

end # module
