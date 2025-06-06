package victor.training.kafka.offerpurchase;

//customers can buy an offer at the desk = recharge + activate
public record OfferPurchasedEvent(long simId, String offerId, int pricePaid) {
}
