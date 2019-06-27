from PIL import Image

img = Image.open('img.JPG')

print(img.format, "%dx%d" % img.size, img.mode)

img.show()
