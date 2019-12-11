$.getJSON("/oauth/loginstatus", function(data) {
	$(function() {		
		handleLoginStatus(data);
		
		var html = $.map(data.persons, function(person) {
			var carddiv = $('<div>', {
				'class' : 'card clickable flex-row flex-xl-nowrap '
			});
			carddiv.attr("personid", person.personid);
			var url = person.image;
			if (!person.image) url = 'img/placeholder.png'
			var cardimg = $('<img>', {
			    'alt'  : '',
			    'class' : 'personphoto',
                'src'  : url
            });
			carddiv.append(cardimg);
			
			var cardname = $('<span>', {
				'class': 'card-name'
			})
			
			cardname.text(person.firstname);
			
			var cardlastname = $('<b>');
			var lastname = person.lastname;
			if (person.infix && person.infix.length != 0) {
				lastname = " " + infix + " " + lastname;
			}
			
			cardlastname.text(lastname);
						
			cardname.append(cardlastname);
						
			carddiv.append(cardname);

			return carddiv;
		});
		
		$('#persons').html(html);
	});
});

