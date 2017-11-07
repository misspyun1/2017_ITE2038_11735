#include "bpt.h"

int main(){

	char * path = "./db.file";
	if(open_db(path)==-1)
		return 0;
	
	int64_t key;
	char * value;
	char instruction;
	int flag;
	
	path = malloc ( 200*sizeof(char));
	value = malloc ( 120* sizeof(char));
	printf("enter the path name : ");
	scanf("%s", path);
	if(open_db(path) == -1)
		exit(EXIT_FAILURE);
	fflush(stdin);
	printf("> ");
	while( scanf("%c", &instruction) != EOF) {
		fflush(stdin);
		switch(instruction){
			case 'i':
				scanf("%ld %s", &key, value);
				flag = insert(key, value);
				if(flag == 0)
					printf("insert success\n");
				else
					printf("insert error : key %ld\n",key);
				break;
			case 'f':
				scanf("%ld", &key);
				value = find(key);
				if(value==NULL)
					printf("find error : key %ld\n",key);
				else
					printf("%s\n",value);
				break;
			case 'd':
				scanf("%ld",&key);
				flag = delete(key);
				if(flag == 0)
					printf("delete success\n");
				else
					printf("delete error : key %ld\n", key);
				break;
		}
		fflush(stdin);
		printf("> ");
	}
	
	return 0;
}
