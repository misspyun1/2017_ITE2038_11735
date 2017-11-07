#include "bpt.h"

int fd;

int cut (int length){
	if(length %2 == 0)
		return length/2;
	else
		return length/2 +1;
}


int open_db ( char * pathname ){
	
	fd = open(pathname, O_RDWR|O_SYNC, 0666 );
	
	if( fd == -1 ) {
		fd = open(pathname, O_RDWR|O_CREAT|O_SYNC, 0666);
		if( fd == -1){
			printf("open db error\n");
			return -1;
		}
		else {
			int64_t idx, first_offset = PAGE_SIZE, root_page_offset = 0;
			int64_t page_num = 4;
			pwrite(fd, &first_offset, 8, 0);//set free page offset == PAGE_SIZE for initallize
			pwrite(fd, &root_page_offset, 8, 8);//set root page offset == 0 for initiallize
			pwrite(fd, &page_num, 8, 16);
			//set page num == 11 for initiallize (header : 1, free page : 10) 
			int i=3;
			while(i--){
				idx = first_offset;
				first_offset += PAGE_SIZE;
				pwrite(fd, &first_offset, 8,idx);
			}
			fsync(fd);
			return 0;
			}
		}
	else return 0;
}

int64_t find_leaf( int64_t root, int64_t key){
		
	int i=0;
    int64_t  c = root, find_point; //c is the offset of leaf that we should find
    	int is_leaf,num_keys; 
	/*is_leaf decide this page is leaf page
	  num_keys is the number of keys in present page*/
	int64_t left_key;// left_key is the present key in present page
	if(root == 0){
		return -1;
	}
	while(true){
		pread(fd,&is_leaf,4, c+8); // read is_leaf
		pread(fd,&num_keys,4,c+12);// read number of keys
		if(is_leaf){
			break; //break if this is the leaf
		}
		i=0;
		//move offset to the first key
		find_point = c+128;
		while(i<num_keys){
			i++;
			pread(fd, &left_key, 8,find_point);
			if(key >= left_key){
				find_point+=16;
			}
			else break;
		}//find the key that bigger than me
		pread(fd, &c, 8, find_point-8);
	}
	return c;
}

char * find (int64_t key){
	
	int i = 0, num_key; //num_key is the number of keys in present node
	
	
	int64_t root, leaf_key;
	/* root is the offset of root page
	   leaf_key is the */
	pread(fd, &root, 8,8);//read offset of root pagei
	if(root ==0) 
		return NULL;
	int64_t c = find_leaf(root, key);//find appropriate leaf page for key
	char * value;
	value = malloc( 120* sizeof(char));

	pread(fd, &num_key, 4, c+12);//read num_key
	
	int64_t find_point = c+128;
	while(i<num_key){
		pread(fd, &leaf_key, 8, find_point);

		if(leaf_key == key)
			break;
		i++;
		find_point +=128;
	}
	if(i==num_key){
		return NULL;
	}
	else{
		pread(fd, value, 120,find_point+8);
		return value;
	}
}


/* make_free_page
 * this function make free page
 */
void  make_free_page ( void ){
	
	int64_t num_page,old_1st_free_page,new_1st_free_page;
	
	pread(fd, &num_page, 8, 16);
	pread(fd, &old_1st_free_page, 8, 0);
	//read number of page and first free page offset

	new_1st_free_page = num_page* PAGE_SIZE;
	// new free page offset will be num_page*PAGE_SIZE
	pwrite(fd, &old_1st_free_page, 8, new_1st_free_page);
	//insert new free page between header page and old_1st_free_page
	pwrite(fd, &new_1st_free_page, 8, 0);

	num_page++;
	pwrite(fd, &num_page, 8, 16);

}


/* make_leaf
 * input : parent_offset of leaf that will be made
 * output : offset of leaf just made
 * it makes leaf, but does not insert record
 * set parent_offset, is_leaf, num_keys
 */
int64_t make_leaf( void ) {
	
	int64_t old_free_page, new_free_page,right_sibling_offset=0, parent_offset = 0;
	int is_leaf = 1,num_keys=0;
		
	pread(fd, &old_free_page, 8, 0);//read free page offset
	pread(fd, &new_free_page, 8, old_free_page);//read next free page offset
	pwrite(fd, &new_free_page,8, 0);//write next free page offset at header page
	//use free page and refil free page
	
	pwrite(fd, &parent_offset, 8, old_free_page); //initiallize the parent_offset == 0
	pwrite(fd, &is_leaf,4,old_free_page+8);//write that it is leaf
	pwrite(fd, &num_keys,4,old_free_page+12);//initiallize the number of keys==0
	pwrite(fd, &right_sibling_offset, 8, old_free_page+120);
	
	make_free_page();
	return old_free_page;
}


int64_t make_internal (void){
	
	int64_t old_free_page, new_free_page,parent_offset = 0;
	int is_leaf = 0,num_keys=0;
	
	pread(fd, &old_free_page, 8, 0);//read free page offset
	pread(fd, &new_free_page, 8, old_free_page);//read next free page offset
	pwrite(fd, &new_free_page,8, 0);//write next free page offset at header page    
	//use free page and refil free page
	
	pwrite(fd, &parent_offset, 8, old_free_page); //initiallize the parent_offset == 0
	pwrite(fd, &is_leaf,4,old_free_page+8);//write that it is not leaf
	pwrite(fd, &num_keys,4,old_free_page+12);//initiallize the number of keys==0
	
	make_free_page();
	return old_free_page;
	
}


/* start_new_tree 
 * input : first key that insert 
 * output : root's offset
 * it makes root and first leaf, but does not insert record
 * set one more page offset == first leaf offset , parent_offset = 0, is_leaf, num_keys == 0
 */
int64_t start_new_tree(int64_t key){
	
	int64_t root,parent_offset=0, first_free_page,first_leaf_offset;
	int num_keys=0;
	
	root = make_leaf();
	
	pwrite(fd, &root, 8, 8); //write root page offset to header page

	pwrite(fd, &parent_offset, 8, root);//write parent offset of root page
	pwrite(fd, &num_keys, 4, root+12);//write number of keys==0 in root page
	
	return root;
}


/* get_left_index
 * input : parent page offset, most left key of child page
 * output : index of child page at parent page base 0
 */
int get_left_index (int64_t parent, int64_t key){
	
	int left_index = 0, num_keys;
	int64_t compare_key, index_point;
	
	index_point = parent + 128;
	pread(fd, &num_keys, 4, parent+12);
	pread(fd, &compare_key, 8, index_point);
	while(left_index <= num_keys && compare_key < key){
		left_index++;
		index_point += 16;
		pread(fd, &compare_key, 8, index_point);
	}

	return left_index;
}

/* get_neighbor_index
 * input : page offset of n
 * output : right_neighbor_index of n
 */
int get_neighbor_index(int64_t n){
	int i, parent_num_keys;
	int64_t parent_offset, child_offset, point_neighbor,compare_key;

	pread(fd, &parent_offset, 8, n);
	pread(fd, &parent_num_keys, 4, parent_offset+12);
	point_neighbor= parent_offset+120;
	pread(fd, &child_offset, 8, point_neighbor);
	for(i = 0; i<=parent_num_keys;i++){
		if(child_offset==n){
			if(i != parent_num_keys)
				return i+1;
			else 
				return -1;
		}
		point_neighbor +=16;
		pread(fd, &child_offset, 8, point_neighbor);
	}
	printf("Search for nonexistent pointer to node in parent.\n");
	exit(EXIT_FAILURE);

}

/* insert_into_node
 * it insert (key + leaf page offset) to internal node
 * output : offset of internal node
 */
int64_t insert_into_internal(int64_t root, int64_t parent, int left_index, int64_t key, int64_t right){
	
	int i,num_keys;
	int64_t compare_key, compare_pointer;
	int64_t insertion_point;

	
	pread(fd, &num_keys, 4, parent+12);
	insertion_point = parent + 128 + (num_keys-1)*16;
	for(i = num_keys; i>left_index; i--) {
		pread(fd, &compare_key, 8, insertion_point);
		pread(fd, &compare_pointer, 8, insertion_point+8);
		pwrite(fd, &compare_key, 8, insertion_point+16);
		pwrite(fd, &compare_pointer, 8, insertion_point+24);
		insertion_point -= 16;
	}//push the (key+offset) back one block
	
	insertion_point = parent + 128 + ((left_index-1)*16);
	num_keys++;
	pwrite(fd, &num_keys, 4, parent+12);
	pwrite(fd, &key, 8, insertion_point);
	pwrite(fd, &right, 8, insertion_point+8);

	return 0;

}

	
/* insert_into_new_root
 * input : two page offset (left, right) which will be the first and second offset
		   key which is the first key of right page
 * output : new root page offset
 */
int64_t insert_into_new_root (int64_t left, int64_t key, int64_t right){
	
	int64_t root = make_internal();
	int num_keys = 1, is_leaf = 0;
	pwrite(fd, &root, 8, 8); // rewrite root page offset to header page
	pwrite(fd, &is_leaf, 4, 12);
	pwrite(fd, &left, 8, root+120);
	pwrite(fd, &key, 8, root+128);
	pwrite(fd, &root, 8, left);
	pwrite(fd, &root, 8, right);
	pwrite(fd, &right, 8, root+ 136);
	pwrite(fd, &num_keys, 4, root+12);
	return 0;
}


int64_t insert_into_parent(int64_t root, int64_t left, int64_t key, int64_t right){
	int left_index, num_keys=2, is_leaf=0, order;
	int64_t parent;

	pread(fd, &parent, 8, left);

	if(parent == 0)
		return insert_into_new_root(left, key, right);

	left_index = get_left_index( parent, key);
	
	pread(fd, &is_leaf, 4, left+8);
	pread(fd, &num_keys, 4, parent+12);

	/* whether left and right is leaf or internal
	 */
	if(is_leaf == 1)
		order = LEAF_ORDER;
	else 
		order = INTERNAL_ORDER;
	
	/* Simple Case :
	 * number of keys in parent is smaller than order
	 * so that just push new key 
	 */
	if(num_keys < order -1)
		return insert_into_internal(root, parent, left_index, key, right);
	
	/* Harder Case : 
	 * split a internal page in order to preserve the properties
	 */
	return insert_into_internal_after_splitting(root, parent, left_index, key, right);

}
/* insert_into_leaf
 * this function actually right record to leaf page
 * output : offset of leaf page
 */
int64_t insert_into_leaf (int64_t leaf, int64_t key, char * value){
	
	int i, insertion_count, num_keys;
	int64_t insertion_point, compare_key;
	char * compare_value;
	insertion_point = leaf + RECORD_SIZE;
	//initiallize the insertion_point to next to the page header of leaf page
	insertion_count = 0;
	pread(fd, &num_keys, 8, leaf+12);
	if(num_keys>0){
		//if there is any key, it can find the place to insert int leaf
		pread(fd, &compare_key, 8, insertion_point);

		while (insertion_count < num_keys && compare_key < key){
			insertion_count++;
			insertion_point += RECORD_SIZE;
			pread(fd, &compare_key, 8, insertion_point);
		}
		
		insertion_point = leaf + (num_keys * RECORD_SIZE);
		for (i = num_keys; i>insertion_count; i--) {
			pread(fd, &compare_key, 8, insertion_point);
			pread(fd, compare_value, 120, insertion_point + 8);
			pwrite(fd, &compare_key, 8, insertion_point + RECORD_SIZE);
			pwrite(fd, compare_value, 120, insertion_point + RECORD_SIZE + 8);
			insertion_point -= RECORD_SIZE;
		}//push the records back one block
	}	
	num_keys++;
	insertion_point = leaf + ((insertion_count + 1) * RECORD_SIZE);
	
	pwrite(fd, &key, 8, insertion_point);
	pwrite(fd, value, 120, insertion_point + 8);
	pwrite(fd, &num_keys, 4, leaf+12);
	return 0;
}


/* insert_into_internal_after_splitting
 * split internal page and (make their new parent page or make right's parent)
 * output : root offset
 */
int64_t insert_into_internal_after_splitting(int64_t root, int64_t old_internal, int left_index, int64_t key, int64_t right){
	
	int i, j, split, old_num_keys, new_num_keys = 0;
	int64_t * temp_keys, * temp_offsets;
	int64_t new_internal, split_point, k_prime, parent_offset, child_offset;
	//k_prime will be the most left key in new_internal

	new_internal = make_internal();
	temp_keys = malloc( (INTERNAL_ORDER+1) * sizeof(int64_t));
	temp_offsets = malloc ( (INTERNAL_ORDER+1) * sizeof(int64_t));

	
	pread(fd, &old_num_keys, 4, old_internal+12);
	
	split_point = old_internal+120;
	for ( i = 0, j = 0; i < old_num_keys+1; i++, j++) {
		if( j == left_index+1) j++;	
		pread(fd, &temp_offsets[j], 8, split_point);
		split_point += 16;
	}

	split_point = old_internal+128;
	for( i = 0, j = 0; i < old_num_keys; i++, j++) {
		if(j == left_index) j++;
		pread(fd, &temp_keys[j], 8, split_point);
		split_point += 16;
	}

	temp_offsets[left_index ] = right;
	temp_keys[left_index-1] = key;

	split = cut (INTERNAL_ORDER - 1);
	old_num_keys = 0;
	split_point = old_internal + 120;
	for(i = 0; i < split ;i++){
		pwrite(fd, &temp_offsets[i], 8, split_point);
		pwrite(fd, &temp_keys[i], 8, split_point + 8);
		split_point += 16;
		old_num_keys++;
	}
	pwrite(fd, &old_num_keys, 4, old_internal+12);
	pwrite(fd, &temp_offsets[i], 8, split_point);
	k_prime = temp_keys[split];
	split_point = new_internal+120;
	for(++i, j = 0; i< INTERNAL_ORDER; i++, j++) {
		pwrite(fd, &temp_offsets[i], 8, split_point);
		pwrite(fd, &temp_keys[i], 8, split_point + 8);
		split_point += 16;
		new_num_keys++;
	}
	pwrite(fd, &new_num_keys, 4, new_internal+12);
	pwrite(fd, &temp_offsets[i], 8, split_point);
	

	free(temp_keys);
	free(temp_offsets);

	pwrite(fd, &parent_offset, 8, new_internal);
	split_point = new_internal+120;
	for(i = 0; i<=new_num_keys; i++){
		pread(fd, &child_offset, 8, split_point);
		pwrite(fd, &new_internal, 8, child_offset);
		split_point+=16;
	}

	return insert_into_parent(root, old_internal, k_prime, new_internal);
}
	
int64_t insert_into_leaf_after_splitting(int64_t root, int64_t leaf, int64_t key, char * value){

	int64_t new_leaf, split_point, compare_key, leaf_right, parent_offset, new_key;
	int64_t * temp_keys,tmp_root;
	char * compare_value;
	char ** temp_values;
	int insertion_index, split, i, j, leaf_num_keys, new_num_keys = 0;

	new_leaf = make_leaf();
	
	temp_keys = malloc((LEAF_ORDER ) * sizeof(int64_t));
	temp_values = malloc((LEAF_ORDER) * sizeof(char *));
	
	for(i=0;i<LEAF_ORDER-1;i++)
		temp_values[i]=malloc(120*sizeof(char));

	insertion_index = 0;
	split_point = leaf + 128;
	pread(fd, &compare_key, 8, split_point );
	while(insertion_index < LEAF_ORDER - 1 && compare_key < key){
		insertion_index++;
		split_point += 128;
		pread(fd, &compare_key, 8, split_point);
	}
	
	pread(fd, &leaf_num_keys, 4, leaf+12);
	split_point = leaf+128;
	for(i = 0, j = 0; i<leaf_num_keys; i++, j++) {
		if(j == insertion_index) j++;
		pread(fd, &temp_keys[j], 8, split_point );
		pread(fd, temp_values[j], 120, split_point +8);
		split_point +=128;
	}
	temp_keys[insertion_index] = key;
	temp_values[insertion_index] = value;

	leaf_num_keys = 0;
	split = cut(LEAF_ORDER - 1);
	split_point = leaf+128;
	for(i = 0; i<split; i++) {
		pwrite(fd, &temp_keys[i], 8, split_point);
		pwrite(fd, temp_values[i], 120, split_point+8);
		split_point += 128;
		leaf_num_keys ++;
	}
	split_point = new_leaf+128;
	for(i = split, j = 0; i< LEAF_ORDER;i++, j++){
		pwrite(fd, &temp_keys[i], 8, split_point );
		pwrite(fd, temp_values[i], 120, split_point +8);
		split_point +=128;
		new_num_keys ++;
	}

	pwrite(fd, &leaf_num_keys, 4, leaf+12);
	pwrite(fd, &new_num_keys, 4, new_leaf+12);
	//write number of keys in each leaf

	for(i=0;i<LEAF_ORDER-1;i++)
		free(temp_values[i]);
	free(temp_values);
	free(temp_keys);
	
	int64_t make_null=leaf+128+128*(leaf_num_keys);
	for(i = leaf_num_keys;i<LEAF_ORDER-1;i++){
		pwrite(fd, NULL, 8, make_null);
		pwrite(fd, NULL, 120, make_null+8);
		make_null+=128;
	}

	pread(fd, &leaf_right, 8, leaf+120);
	pwrite(fd, &leaf_right, 8, new_leaf +120);
	pwrite(fd, &new_leaf, 8, leaf+120);
	//set leaf pointing new_leaf

	pread(fd, &parent_offset, 8, leaf);
	pwrite(fd, &parent_offset, 8, new_leaf);
	pread(fd, &new_key, 8, new_leaf+128);
	/* set new_leaf  parent == leaf parent
	 * new_key is the most left key in new_leaf
	 */

	return insert_into_parent(root, leaf, new_key, new_leaf);


	
}

int insert (int64_t key, char * value){
	
	int64_t root_page, leaf_page,chk;
	int leaf_num_keys;
	if(find(key) != NULL)//it does not allow duplication
		return -1;

	pread(fd, &root_page, 8, 8);
	
	if(root_page == 0)
		root_page = start_new_tree(key);
	//if there is no tree yet, start new tree

	leaf_page = find_leaf(root_page, key);
	//find the appropriate leaf page

	/* when leaf is not full now, it's easy to insert records
	 * so we just insert_into_leaf when leaf_num_key is smaller than LEAF_ORDER -1
	 */
	pread(fd, &leaf_num_keys, 4, leaf_page+12);
	if(leaf_num_keys < LEAF_ORDER - 1){
		insert_into_leaf (leaf_page, key, value);
		return 0; 
	}

	return insert_into_leaf_after_splitting(root_page, leaf_page, key, value);
}

int64_t remove_entry_from_page( int64_t now_page, int64_t key){
	int i, num_keys, is_leaf,idx;
	int64_t compare_key,  remove_point, data_size, compare_offset,parent_offset;
	char * compare_value;
	compare_value = malloc(120*sizeof(char));

	i = 0;
	pread(fd, &num_keys, 4, now_page+12);
	pread(fd, &is_leaf, 4, now_page+8);
	pread(fd, &parent_offset, 8, now_page);
	
	if(is_leaf)
		data_size = 128;
	else
		data_size = 16;

	remove_point = now_page+128;
	pread(fd, &compare_key, 8, remove_point);
	
	while( compare_key != key){
		i++;
		remove_point += data_size;
		pread(fd, &compare_key, 8, remove_point);
		}
	remove_point +=data_size;
	if(is_leaf){
		if(i == 0){
			idx = get_left_index(parent_offset, key);
			pwrite(fd, &compare_key, 8, parent_offset+128+(i-1)*16);
		}
		for(++i;i<num_keys;i++){
			pread(fd, &compare_key, 8, remove_point);
			pread(fd, compare_value, 120, remove_point+8);
			pwrite(fd, &compare_key, 8, remove_point-128);
			pwrite(fd, compare_value, 120, remove_point-120);
			remove_point += data_size;
		}	
	}
	else {
		for(++i; i<num_keys;i++){
			pread(fd, &compare_key, 8, remove_point);
			pread(fd, &compare_offset, 8, remove_point+8);
			pwrite(fd, &compare_key, 8, remove_point-16);
			pwrite(fd, &compare_offset, 8, remove_point-8);
			remove_point += data_size;
		}
	}
	num_keys--;
	pwrite(fd, &num_keys, 4, now_page+12);
	return now_page;
}

int64_t adjust_root(int64_t root){
	
	int64_t new_root, i_am_groot=0;
	int num_keys,is_leaf;

	pread(fd, &is_leaf, 4, root+8);
	pread(fd, &num_keys, 4, root+12);
	if(num_keys > 0)
		return root;

	if(is_leaf == 0){
		pread(fd, &new_root, 8, root+120);
		pwrite(fd, &i_am_groot, 8, new_root);
		pwrite(fd, &new_root, 8, 8);
	}
	
	return new_root;
}

/* coalesce_nodes
 * input : root, 
 		   now, neighbor -> neighbor has bigger keys than now
 		   neighbor_index -> neighbor is in (neighbor_index)th of parent 
 		   				  -> if now is right most, neighbor_index : 1 else, neighbor_index	
 		   k_prime -> the key between two offset(now, neighbor)
 * output : root offset
 * coalesce_node make two node (now, neighbor) one
 */
int64_t coalesce_nodes( int64_t root, int64_t now, int64_t neighbor, int neighbor_index, int64_t k_prime){
	//neighbor has bigger keys than now

	int i, j, is_leaf, neighbor_end, neighbor_num_keys, now_num_keys, now_insertion_index;
	int64_t parent_offset,tmp, neighbor_insertion_point, now_insertion_point;
	int64_t compare_key, compare_offset;
	char * compare_value;

	compare_value = malloc(120*sizeof(char));

	if(neighbor_index == -1){
		tmp = now;
		now = neighbor;
		neighbor = tmp;
	}

	pread(fd, &now_num_keys, 4, now+12);
	pread(fd, &neighbor_num_keys, 4, neighbor+12);
	pread(fd, &is_leaf, 4, now+8);
	



	now_insertion_index = now_num_keys;
	
	if(is_leaf == 0){
		now_insertion_point = now + 128 + (now_num_keys*16);
		pwrite(fd, &k_prime, 8, now_insertion_point);
		pread(fd, &compare_offset, 8, neighbor+120);
		pwrite(fd, &compare_offset, 8, now_insertion_point+8);
		//make k_prime and neighbor's one more offset a pair of (key+offset)

		now_num_keys++;
		pwrite(fd, &now_num_keys, 4, now+12);
		
		neighbor_end = neighbor_num_keys;

		neighbor_insertion_point = neighbor+128;
		now_insertion_point += 16;
		for(i = now_insertion_index+1, j= 0; j<neighbor_end;i++, j++){
			pread(fd, &compare_key, 8, neighbor_insertion_point);
			pread(fd, &compare_offset, 8, neighbor_insertion_point+8);
			pwrite(fd, &now, 8, compare_offset); 
			// now all childeren of neighbor point up 'now' for parent
			pwrite(fd, &compare_key, 8, now_insertion_point);
			pwrite(fd, &compare_offset, 8, now_insertion_point+8);
			neighbor_insertion_point += 16;
			now_insertion_point += 16;

			now_num_keys++;
			neighbor_num_keys--;
		}//move all keys and offsets of 'neighbor' to 'now'
		pwrite(fd, &now_num_keys, 4, now+12);
		pwrite(fd, &neighbor_num_keys, 4, neighbor+12);
	}

	else{
		neighbor_insertion_point = neighbor+128 ;
		now_insertion_point = now + 128 *(now_num_keys +1);
		for(i = now_insertion_index, j = 0; j< neighbor_num_keys; i++, j++){
			pread(fd, &compare_key, 8, neighbor_insertion_point);
			pread(fd, compare_value, 120, neighbor_insertion_point + 8);
			pwrite(fd, &compare_key, 8, now_insertion_point);
			pwrite(fd, compare_value, 120, now_insertion_point+8);
			
			neighbor_insertion_point += 128;
			now_insertion_point += 128;

			now_num_keys++;
			neighbor_num_keys--;
		}
		pwrite(fd, &now_num_keys, 4, now+12);
		pwrite(fd, &neighbor_num_keys, 4, neighbor+12);
	}
	pread(fd, &parent_offset, 8, now);
	root = delete_entry(root,  parent_offset, k_prime);

	return root;
}
	
/* redistribute_nodes
 * input : root
 		   now, neighbor -> neighbor has bigger keys than now
 		   neighbor_index -> neighbor is in (neighbor_index)th of parent 
 		   				 -> if now is right most, neighbor_index : 1 else, neighbor_index	
 		   k_prime -> the key between two offset(now, neighbor)
 		   k_prime_index -> similar to neighbor_index
 		   				-> but it return parent_num_keys-1 when now is right most
 * output : root offset
 * it move neighbor's data to now
 */
int64_t redistribute_nodes( int64_t root, int64_t now, int64_t neighbor, int neighbor_index, int k_prime_index, int64_t k_prime)	
{
	int i,is_leaf, now_num_keys, neighbor_num_keys;
	int64_t tmp, compare_key, compare_offset, now_insertion_point, parent_offset, neighbor_insertion_point, data_size;
	char * compare_value;

	compare_value = malloc(120*sizeof(char));

	pread(fd, &is_leaf, 4, now+8);
	pread(fd, &now_num_keys, 4, now+12);
	pread(fd, &neighbor_num_keys, 4, neighbor+12);

	if(neighbor_index !=-1){
		if(is_leaf == 0){
			neighbor_insertion_point = neighbor+128;
			now_insertion_point = now+128+16*now_num_keys;
			pread(fd, &parent_offset, 8, now);
			parent_offset +=  (128 + 16*(k_prime_index-1));
	
			pread(fd, &compare_key, 8, neighbor_insertion_point);
			pread(fd, &compare_offset, 8, neighbor+120);
			
			pwrite(fd, &neighbor, 8, compare_offset);			
			pwrite(fd, &compare_key, 8, parent_offset);
			//rewrite (neighbor_index)th key of parent to neighbor key[0]
			pwrite(fd, &k_prime, 8, now_insertion_point);
			pwrite(fd, &compare_offset, 8, now_insertion_point+8);
			//write (k_prime, compare_offset[one more offset of neighbor]) par to last of now

			
			//move first key of neighbor to the last of now;
			for(i = neighbor_num_keys; i>1;i--){
				pread(fd, &compare_key, 8, neighbor_insertion_point+16);
				pread(fd, &compare_offset, 8, neighbor_insertion_point+24);
				pwrite(fd, &compare_key, 8, neighbor_insertion_point );
				pwrite(fd, &compare_offset, 8, neighbor_insertion_point+8);
				neighbor_insertion_point +=16;
			}
		}
		else{
			neighbor_insertion_point = neighbor+128;
			now_insertion_point = now+128+128*now_num_keys;
			pread(fd, &parent_offset, 8, now);
			parent_offset += (128+16*(k_prime_index-1));

			
			pread(fd, &compare_key, 8, neighbor_insertion_point);
			pread(fd, compare_value, 120, neighbor_insertion_point+8);
			pwrite(fd, &compare_key, 8, now_insertion_point);
			pwrite(fd, compare_value, 120, now_insertion_point+8);

			
			for(i = neighbor_num_keys; i > 1; i--){
				pread(fd, &compare_key, 8, neighbor_insertion_point+128);
				pread(fd, compare_value, 120, neighbor_insertion_point+136);
				pwrite(fd, &compare_key, 8, neighbor_insertion_point);
				pwrite(fd, compare_value, 120, neighbor_insertion_point+8);
				neighbor_insertion_point += 128;
			}
			pread(fd, &compare_key, 8, neighbor+128);
			pwrite(fd, &compare_key, 8, parent_offset);
		}
	
	}

	else {
		if(is_leaf == 0){
			now_insertion_point = now + 128;
			neighbor_insertion_point = neighbor + 128 + 16*(neighbor_num_keys-1);
			pread(fd, &parent_offset, 8, now);
			parent_offset +=  (128 + 16*(k_prime_index));
				
			//move first key of neighbor to now;
			for(i = neighbor_num_keys; i>0;i--){
				pread(fd, &compare_key, 8, now_insertion_point);
				pread(fd, &compare_offset, 8, now_insertion_point+8);
				pwrite(fd, &compare_key, 8, now_insertion_point+16 );
				pwrite(fd, &compare_offset, 8, now_insertion_point+24);
				now_insertion_point +=16;
			}
			pread(fd, &compare_key, 8, neighbor_insertion_point);
			pread(fd, &compare_offset, 8, now+120);
			//for now, compare_key is the first key of 'neighbor' 
			//and compare_offset is one more offset of 'now'
			pwrite(fd, &k_prime, 8, now+128);
			pwrite(fd, &compare_offset, 8, now+136);
			//make (k_prime, one more offset of 'now') pair 
			//they will be the first (key, offset)pair of 'now'
			pread(fd, &compare_offset,8, neighbor_insertion_point+8 );
			pwrite(fd, &compare_key, 8, parent_offset);
			pwrite(fd, &compare_offset, 8, now+8);
		}
		else{
			neighbor_insertion_point = neighbor+128+128*(neighbor_num_keys-1);
			now_insertion_point = now +128;
			pread(fd, &parent_offset, 8, now);
			parent_offset +=(128+16*(k_prime_index));

			
			for(i = neighbor_num_keys;i>0;i--){
				pread(fd, &compare_key, 8, now_insertion_point);
				pread(fd, compare_value, 120, now_insertion_point+8);
				pwrite(fd, &compare_key, 8, now_insertion_point+128);
				pwrite(fd, compare_value, 120, now_insertion_point+136);
				now_insertion_point +=16;
			}

			pread(fd, &compare_key, 8, neighbor_insertion_point);
			pread(fd, compare_value, 120, neighbor_insertion_point+8);

			pwrite(fd, &compare_key, 8, now+128);
			pwrite(fd, compare_value, 120, now+136);
			pwrite(fd, &compare_key, 8, parent_offset);

		}
	}
	neighbor_num_keys --;
	now_num_keys++;
	pwrite(fd, &neighbor_num_keys, 4, neighbor+12);
	pwrite(fd, &now_num_keys, 4, now+12);

	return root;
}

int64_t delete_entry(int64_t root, int64_t now, int64_t key){
	int64_t k_prime;
	int64_t neighbor,parent,parent_insert_point,right_sibling;
	int min_keys, now_num_keys,neighbor_num_keys, parent_num_keys, is_leaf, neighbor_index, k_prime_index, capacity;

	now = remove_entry_from_page(now, key);

	if(now== root)
		return adjust_root(root);

	pread(fd, &now_num_keys, 4, now+12);
	pread(fd, &is_leaf, 4, now+8);
	pread(fd, &parent, 8, now);
	pread(fd, &parent_num_keys, 8, parent+12);

	if(is_leaf ==0){
		min_keys = cut(LEAF_ORDER)-1;
		capacity = LEAF_ORDER;
		pread(fd, &right_sibling, 8, now+120);
	}
	else{
		min_keys = cut(INTERNAL_ORDER)-1;
		capacity = INTERNAL_ORDER;
	}//min_keys and capacity will be different whether 'now' is leaf


	if(now_num_keys>=min_keys)
		return root;

	neighbor_index = get_neighbor_index(now);
	if(neighbor_index == -1){
		k_prime_index = parent_num_keys-1;
		parent_insert_point = parent+ 128+16*(k_prime_index);
		pread(fd, &neighbor, 8, parent_insert_point-8);
	}
	else{
		k_prime_index = neighbor_index;
		parent_insert_point = parent+128+16*(k_prime_index-1);
		pread(fd, &neighbor, 8, parent_insert_point+8);
	}// define k_prime_index, neighbor offset

	pread(fd, &k_prime, 8, parent_insert_point);
	pread(fd, &neighbor_num_keys, 4, neighbor+12);
	pread(fd, &now_num_keys, 4, now+12);\
	if(is_leaf == 1){
		if(neighbor!=right_sibling)
			printf("i am leaf, but my neighbor is not my right sibling!!\n");
	}

	/* we did delete the data
	 * now, we have to adjust the nodes
	 * so when now_num_keys+neighbor_num_keys is too small we just coalescence
	 * but when that value is too big we have to redistribute
	 */

	if(now_num_keys+neighbor_num_keys < capacity)
		return coalesce_nodes(root, now, neighbor, neighbor_index, k_prime);
	else
		return redistribute_nodes(root, now, neighbor, neighbor_index, k_prime_index, k_prime);
}

int delete (int64_t key){
	int64_t root, key_leaf;
	char * key_value;
	key_value = malloc(120*sizeof(char));

	pread(fd, &root, 8, 8);
	key_value = find(key);
	key_leaf = find_leaf(root, key);
	if(key_value != NULL && key_leaf !=-1){
		root = delete_entry(root, key_leaf, key);
		return 0;
	}
	return -1;
}



