file = open("tt.txt","w")
file.seek(7)
file.write("i")

file.seek(0)
file.write("s")
file.flush()
file.close()