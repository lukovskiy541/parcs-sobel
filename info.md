copy to local:
docker cp klt-master-qzxy:/tmp/output_sobel.pgm ~/output_sobel.pgm
gcloud compute scp master:~/output_sobel.pgm .


img to pgm:

magick input.png -colorspace Gray -depth 8 input.pgm
