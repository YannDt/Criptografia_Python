##########
# INICIO #
##########

nomearquivo="msg.txt"

escolha=input("Digite 'c' se deseja criptografar uma mensagem ou 'd' para descriptografar uma mensagem: ") 
while escolha!="c" and escolha!="d":
    print("\nCaractere invalído, digite 'c' para criptografar ou 'd' para descriptografar!")
    escolha=input("Tente novamente: ")
    
################
# CRIPTOGRAFIA #
################

if escolha=="c":
    msg=input("\nMensagem: ")
    while len(msg)>128:
        print("\nA mensagem deve conter até 128 caracteres, foram digitados ",len(msg),". Tente novamente.")
        msg=input("\nMensagem: ")
    c='' 
    for i in range(len(msg)): 
      if i%2==0:
        c=c.join(chr(ord(msg[i])+2))
      else:
        c=c.join(chr(ord(msg[i])-3))
    arquivo=open(nomearquivo,"w") 
    arquivo.write(c)
    arquivo.close()
    print("\n=======================================================\n")
    print("Arquivo '",nomearquivo,"' criado e criptografado!\n")
    print("=======================================================\n")
    print("Criptografia: ", c)

###################
# DESCRIPTOGRAFIA #
###################

if escolha=="d":
    arquivo=open(nomearquivo,"r")
    msg=arquivo.read()
    desc=arquivo.readlines()
    arquivo.close
    d = ''
    for i in range(len(msg)):
      if i%2==0:
        d=d.join(chr(ord(msg[i])-2))
      else:
        d=d.join(chr(ord(msg[i])+3))
    arquivo=open(nomearquivo,"w")
    desc.append(d)
    arquivo.writelines(desc)
    arquivo.close()
    print("\n=======================================================\n")
    print("Arquivo '",nomearquivo,"' descriptografado!\n")
    print("=======================================================\n")
    print("Descriptografia: ",d)
