$.getJSON("/oauth/loginstatus", function(data) {
	$(function() {
		handleLoginStatus(data);
		 
		var html = $.map(data.scopes, function(scope) {
			 var carddiv = $('<div class="scope-div">');

		 	 var cardheader = $('<h6>', {'class': 'scope-header'});
			 cardheader.text(scope.title);
			 carddiv.append(cardheader);
			 
			 var cardblock = $('<div>', {'class': 'scope-text'})
			 cardblock.text(scope.description);
			 carddiv.append(cardblock);
			 			 
			 
			 return carddiv;
		 });
		 $('#scopes').html(html);
	});
});

